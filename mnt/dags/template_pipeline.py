import os
import logging
import csv
import pandas as pd
import psycopg2
import boto3
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
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



    
    



# Define your DAG
with DAG(
    dag_id='pipeline_full_v1',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
    start_date=datetime(2023, 5, 25),  # Set the start date of the DAG
)as dags:
    
    start = DummyOperator(task_id="start")

    fetch_from_database = SqlToS3Operator(
        task_id="fetch_from_database",
        sql_conn_id='pg_container',
        query='SELECT * FROM dbo.table_product_demand',
        aws_conn_id="minio",
        s3_bucket='datalake',
        s3_key=f"src/table_product_demand.csv",
        replace=True,
    )

    transform_s3 = AthenaOperator(
        task_id="transform_s3",
        aws_conn_id="minio",
        database="datalake/src",
        query= f"SELECT * FROM table_product_demand WHERE date >= '{{{{ds}}}}' AND date < '{{{{next_ds_nodash}}}}'",
        output_location=f"src/session/{{{{execution_date.strftime('%Y/%m')}}}}/table_product_demand_{{{{ds}}}}.csv"


    )


    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> fetch_from_database  >> transform_s3 >> end
