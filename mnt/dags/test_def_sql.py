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
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
    

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup' : False
}
s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'

   

with DAG(
    'test_def_sql',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
    start_date=datetime(2023, 5, 18),  # Set the start date of the DAG
)as dags:
    
    start = DummyOperator(task_id="start")

    sql_to_s3_task = SqlToS3Operator(
        task_id="sql_to_s3_task",
        sql_conn_id='pg_container',
        query=f"SELECT * FROM dbo.table_product_demand WHERE date >= '{{{{ds}}}}' AND date < '{{{{next_ds_nodash}}}}'",
        #"SELECT * FROM dbo.table_product_demand WHERE date >= {{ds_nodash}} and date < {{next_ds_nodash}}",
        aws_conn_id="minio",
        s3_bucket='datalake',
        s3_key=f"src_test/table_product_demand_{{{{ds}}}}.csv",
        file_format="csv",
        pd_kwargs={"index": False},
        replace=True,
    )

    # from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
    transform_to_demand = S3FileTransformOperator(
        task_id='transform_to_demand',
        source_s3_key=f's3://datalake/src_test/table_product_demand_{{{{ds}}}}.csv',
        dest_s3_key=f's3://datalake/src_test_demand/table_material_demand_{{{{ds}}}}.csv',
        #transform_script='cp',
        select_expression=f"SELECT product, demand FROM table_product_demand_2023-05-27.csv",      
        source_aws_conn_id='minio',
        dest_aws_conn_id='minio',
        replace=True,
        )

    # transform_s3 = PythonOperator(
    #     task_id='transform_s3',
    #     python_callable=_transform
    # )


    # upload_to_postgres = S3ToSqlOperator(
    #     task_id='upload_to_postgres',
    #     s3_bucket='minio',
    #     s3_key=f'{{ execution_date.strftime("%Y-%m-%d") }}.csv',
    #     table='your_table',
    #     postgres_conn_id='pg_container',
    #     replace=True,
    # )

    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> sql_to_s3_task  >> transform_to_demand >> end
