import os
import logging
import csv
import pandas as pd
import psycopg2
import boto3
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
import boto3


from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator


s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'



default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup' : False,
    ######## w8 previous task ##########
    'wait_for_downstream' : True,
    'depends_on_past':True,
    ######## w8 previous task ##########
    'catchup' : False, 
}
# Define your DAG
with DAG(
    dag_id='01_database_to_datalake',
    default_args=default_args,
    description='Copy file from PostgreSQL(database) to MinIO(datalake), the transform and load will be in another dag file',
    schedule_interval='@daily',  # Set your desired schedule interval '@daily'
    start_date=datetime(2023, 4, 25),  # Set the start date of the DAG

)as dags:
    
    start = DummyOperator(task_id="start")

    fetch_from_database = SqlToS3Operator(
        task_id="fetch_from_database",
        sql_conn_id='pg_container',
        query=f"SELECT * FROM dbo.table_product_demand where date >= '{{{{ds}}}}' and date < '{{{{next_ds}}}}' ", #<<--- Basically copy everything
        aws_conn_id="minio",
        s3_bucket='datalake',
        s3_key=f"src/session_raw_data/{{{{execution_date.strftime('%Y/%m')}}}}/table_product_demand_{{{{ds}}}}.csv",
        replace=True,
        file_format="csv",
        pd_kwargs={"index": False} #<<---- if True, they will be another column containing numbers
    )
   

    

    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> fetch_from_database >> end
    