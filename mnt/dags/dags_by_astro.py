import os
from datetime import datetime
#from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

# Import decorators and classes from the SDK
from airflow.decorators import dag
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
from astro.databases.postgres import PostgresDatabase


homes_data1 = aql.load_file(
    task_id="load_homes1",
    input_file=File(path="s3://airflow-kenten/homes1.csv", conn_id='pg_container'),
    output_table=Table(name="HOMES1", conn_id='minio')
)