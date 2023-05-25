from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
#from airflow.providers.postgres.operators.postgres_to_s3 import PostgresToS3Operator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from postgres_to_S3 import (
    _fetch_from_postgres
    #_transform_data_in_S3

)

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    dag_id='coffee_data_pipline',
    default_args=default_args,
    start_date=datetime(2023, 2, 8),
    schedule_interval='@daily'
) as dag:
    
    start = DummyOperator(task_id="start")

    fetch_from_postgres = PythonOperator(
            task_id='fetch_from_postgres',
            python_callable=_fetch_from_postgres
    )

    transform_data_in_S3 = DummyOperator(
        task_id='transform_data_in_S3',
       #python_callable=_transform_data_in_S3,
       # provide_context=True
    )

    end = DummyOperator(task_id='end')

    start >> fetch_from_postgres >> end