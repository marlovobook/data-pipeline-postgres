import os
import logging
import csv
import pandas as pd
import psycopg2
import boto3
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
import boto3
from airflow.models import Variable

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.mysql.transfers.s3_to_mysql import S3ToMySqlOperator

default_args = {
    'owner' : 'BOOK',
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    'catchup' : False
}
s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'
s3_key='src/table_product_demand.csv'


def download_from_s3():
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/table_material_demand.csv'
    
    # Download CSV file from S3
    local_path = 'dags/temp'
    #f'/temp/session/{{{{ds}}}}.csv'
    file_name = s3_hook.download_file(
        key=s3_key,
        bucket_name=s3_bucket,
        local_path=local_path,
        #preserve_file_name=True,
        )
    
    return file_name

def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

session_folder = f"src/session/{{{{execution_date.strftime('%Y/%m')}}}}/table_product_demand_{{{{ds}}}}.csv"

def transform_product_to_material(ds, next_ds, data_interval_start):
    # Read CSV file using Pandas
    df = pd.read_csv('dags/temp/table_material_demand.csv', index_col=False)
    # # Perform query on the data 
    df = df.query(f"date >= '{ds}' and date < '{next_ds}'")
    #df = pd.to_datetime(df['date'])
    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv'
    df.to_csv(query_result_csv, index=False)
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook.load_file(
        filename=query_result_csv,
        key=f"src/session/{ds_str}/table_material_demand_{ds}.csv",
        #key=f"src/session/{ds.strftime('%Y/%m')}/table_product_demand_{ds}.csv",
        bucket_name='datalake',
        replace=True
          )

def _download_file_from_datalake(ds, data_interval_start):

    # Download File from S3
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook = S3Hook(aws_conn_id="minio")
    file_name_material = s3_hook.download_file(
        key=f"src/session/{ds_str}/table_material_demand_{ds}.csv",
        bucket_name="datalake",
    )

    return file_name_material

def  _load_data_into_data_warehouse(**context):
    
    postgres_hook = PostgresHook(postgres_conn_id="pg_container")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from Xcoms
    file_name_material = context["ti"].xcom_pull(task_ids="download_transformed_file_from_datalake", key="return_value")

    # Copy file to datawarehouse in this case is postgres
    postgres_hook.copy_expert(

        """
            COPY
                dbo.table_material_demand

            FROM STDIN DELIMITER ',' CSV HEADER
    
        """,
        file_name_material,
    

    )

# Define your DAG
with DAG(
    dag_id='pipeline_full_v2',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
    start_date=datetime(2023, 4, 20),  # Set the start date of the DAG
)as dags:
    
    start = DummyOperator(task_id="start")
   
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,  
    )

    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'table_material_demand.csv'
        }
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_transfromed_files_to_s3',
        python_callable=transform_product_to_material,
    )

    download_transformed_file_from_datalake = PythonOperator(
        task_id="download_transformed_file_from_datalake",
        python_callable=_download_file_from_datalake
    )

    create_table_in_data_warehouse = PostgresOperator(
        task_id='create_table_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql="""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand (
                date DATE,
                shop_id VARCHAR(100),
                raw_material VARCHAR(100),
                demand_kg VARCHAR(100)
            )
        
        """,
        )
    
    load_data_into_data_warehouse  = PythonOperator(
        task_id="load_data_into_data_warehouse",
        python_callable=_load_data_into_data_warehouse
    )



    end = DummyOperator(task_id='end')

    # Set task dependencies
    #fetch_from_database file to big temp stop

    ##download from datalake -> local for transforming -> postgres (as a datawarehouse)
    start  >> task_download_from_s3 >> task_rename_file >> task_upload_to_s3 >> download_transformed_file_from_datalake

    download_transformed_file_from_datalake >> create_table_in_data_warehouse >> load_data_into_data_warehouse >> end