import os
import logging
import csv
import pandas as pd
import psycopg2
import boto3
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
#from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator
#from airflow.providers.amazon.aws.operators.athena import AthenaOperator



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


def transform_material_by_date(ds, next_ds, data_interval_start):
    # Read CSV file using Pandas
    df = pd.read_csv('dags/temp/table_material_demand.csv', index_col=False)


    # -------------------------Key code -------------------------#

    # # Perform query on the data 
    #query the date that >= start_date of the dag but < today_date
    df = df.query(f"date >= '{ds}' and date < '{next_ds}'")

    # -------------------------Key code -------------------------#

    #df = pd.to_datetime(df['date'])
    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv' #<<--- the next day file will replace this, so you won't have multiply .csv
    df.to_csv(query_result_csv, index=False)

    '''
    this code will CREATE partition for each year and month in S3
    for example, Folder 2022, folder 2023
    and each year folder will have month folder: 01 02 03 to 12
    The file table_material_demand_2023-05-18.csv will be in
            datalake/session/2023/05/table_material_demand_2023-05-18.csv
    
    '''
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
    data_interval_start = context["data_interval_start"]
    postgres_hook = PostgresHook(postgres_conn_id="pg_container")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from Xcoms
    file_name_material = context["ti"].xcom_pull(task_ids="download_transformed_file_from_datalake", key="return_value")
    ds_str = data_interval_start.strftime('%Y_%m')
    # Copy file to datawarehouse in this case is postgres
    postgres_hook.copy_expert(

        f"""
            COPY
                dbo.table_material_demand_{ds_str}

            FROM STDIN DELIMITER ',' CSV HEADER
    
        """,
        file_name_material,
    

    )

# Define your DAG ##
default_args = {
    'owner' : 'BOOK',
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    ######## w8 previous task ##########
    'wait_for_downstream' : True,
    'depends_on_past':True,
    ######## w8 previous task ##########
    'catchup' : False, 
}
with DAG(
    dag_id='02_datalake_to_datawarehouse',
    default_args=default_args,
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

    upload_transfromed_files_to_s3 = PythonOperator(
        task_id='upload_transfromed_files_to_s3',
        python_callable=transform_material_by_date,
    )

    download_transformed_file_from_datalake = PythonOperator(
        task_id="download_transformed_file_from_datalake",
        python_callable=_download_file_from_datalake
    )

    ### dbo.table_material_demand_2023_05
    ## 1 table per month
    """
    dbo.table_material_demand_{{{{data_interval_start.strftime('%Y_%m')}}}}
    if today is 2023-02-15
    create table table_material_demand_2023_02
    in schema dbo
    ### the next dah will insert data of the today into this table
    """
    create_table_in_data_warehouse = PostgresOperator(
        task_id='create_table_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand_{{{{data_interval_start.strftime('%Y_%m')}}}} (
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

    ##download from datalake -> local for transforming -> postgres (as a datawarehouse)
    start  >> task_download_from_s3 >> task_rename_file >> upload_transfromed_files_to_s3 >> download_transformed_file_from_datalake

    download_transformed_file_from_datalake >> create_table_in_data_warehouse >> load_data_into_data_warehouse >> end