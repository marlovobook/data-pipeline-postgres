import os
import logging
import csv

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

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup' : False
}
s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'


def _transform(ds, next_ds_nodash):

    file_name = 'src/table_product_demand.csv'

    # Create an S3 client
    client = boto3.client('s3')
    
    response = client.select_object_content(
        Bucket=bucket_name,
        Key=file_name,
        Expression=f"SELECT * FROM {file_name} WHERE date >= '{ds}' AND date < '{next_ds_nodash}'",
        ExpressionType='SQL',
        InputSerialization={'CSV': {'FileHeaderInfo': 'USE'}},
        OutputSerialization={'CSV': {}}
    )
    data = []

    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            # Process records (convert to desired format or store in a list)
            data.append(records)