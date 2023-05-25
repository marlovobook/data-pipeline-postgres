import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}




# Define your DAG
with DAG(
    'test',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval=None,  # Set your desired schedule interval
    start_date=datetime(2023, 5, 18),  # Set the start date of the DAG
)as dags:
    
    start = DummyOperator(task_id="start")

    sql_to_s3_task = SqlToS3Operator(
        task_id="sql_to_s3_task",
        sql_conn_id='pg_container',
        query='SELECT * FROM dbo.table_product_demand',
        #"SELECT * FROM dbo.table_product_demand WHERE date >= {{ds_nodash}} and date < {{next_ds_nodash}}",
        aws_conn_id="minio",
        s3_bucket='datalake',
        s3_key=f"src/table_product_demand.csv",
        replace=True,
    )

    

    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> sql_to_s3_task >> end
