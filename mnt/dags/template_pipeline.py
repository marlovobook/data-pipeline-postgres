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
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup' : False
}
s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'
s3_key='src/table_product_demand.csv'


def download_from_s3():
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/table_product_demand.csv'
    
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
    df = pd.read_csv('dags/temp/table_product_demand.csv', index_col=False)
    # Perform query on the data 
    df = df.query(f"date >= '{ds}' and date < '{next_ds}'")
    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv'
    df.to_csv(query_result_csv, index=False)
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook.load_file(
        filename=query_result_csv,
        key=f"src/session/{ds_str}/table_product_demand_{ds}.csv",
        #key=f"src/session/{ds.strftime('%Y/%m')}/table_product_demand_{ds}.csv",
        bucket_name='datalake',
        replace=True
          )


# Define your DAG
with DAG(
    dag_id='pipeline_full_v1',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
    start_date=datetime(2023, 5, 25),  # Set the start date of the DAG
)as dags:
    
    start = DummyOperator(task_id="start")

    # fetch_from_database = SqlToS3Operator(
    #     task_id="fetch_from_database",
    #     sql_conn_id='pg_container',
    #     query='SELECT * FROM dbo.table_product_demand',
    #     aws_conn_id="minio",
    #     s3_bucket='datalake',
    #     s3_key=f"src/table_product_demand.csv",
    #     replace=True,
    #     file_format="csv",
    #     pd_kwargs={"index": False}
    # )
   
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,  
    )

    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'table_product_demand.csv'
        }
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=transform_product_to_material,
    )

    end = DummyOperator(task_id='end')

    # Set task dependencies
    #fetch_from_database file to big temp stop
    start  >> task_download_from_s3 >> task_rename_file >> task_upload_to_s3 >> end
