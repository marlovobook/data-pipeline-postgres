import os
import logging
import csv
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile


from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator


# s3_hook = S3Hook(aws_conn_id='minio')
# postgres_hook = PostgresHook(postgres_conn_id='pg_container')
# bucket_name = 'datalake'

def _load_data():

    postgres_hook = PostgresHook(postgres_conn_id='pg_container')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    #cursor.execute("SET datestyle = 'ISO, DMY';")
    
    file_name = 'dags/temp/online_retail_origin.csv'

    postgres_hook.copy_expert(

        """
            COPY 
                dbo.table_online_retail_origin(id, Invoice, StockCode, Description,Quantity,InvoiceDate,Price,Customer_ID,Country,last_updated)
            
            FROM STDIN DELIMITER ',' CSV HEADER

        """,
        file_name,

    )
    # conn.commit()
    # cursor.close()
    # conn.close()



default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'catchup' : False,
    ######## w8 previous task ##########
    'wait_for_downstream' : True,
    'depends_on_past':True,
    ######## w8 previous task ##########
    'catchup' : False, 
}
# Define your DAG
with DAG(
    dag_id='01_retail_origin',
    default_args=default_args,
    description='Copy online_retail_origin file from local',
    schedule_interval=None,  # Set your desired schedule interval '@daily'
    start_date=datetime(2023, 4, 25),  # Set the start date of the DAG

)as dags:
    
    start = DummyOperator(task_id="start")

    upload_retail_origin = PostgresOperator(
        task_id='create_online_retail_origin_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            DROP TABLE IF EXISTS dbo.table_online_retail_origin;

            

            CREATE TABLE dbo.table_online_retail_origin (
                id INT,
                Invoice VARCHAR(100),
                StockCode VARCHAR(100),
                Description VARCHAR(100),
                Quantity INT,
                InvoiceDate TIMESTAMP,
                Price FLOAT,
                Customer_ID VARCHAR(100),
                Country VARCHAR(100),
                last_updated TIMESTAMP,
                constraint table_online_retail_origin_pk primary key (id)
            );

            
        
        """,
    )

   #D:\Docker\demo_etl\postgresql\data\online_retail_origin.csv

    #COPY dbo.table_online_retail_origin(id, Invoice, StockCode, Description,Quantity,InvoiceDate,Price,Customer_ID,Country,last_updated)
         #FROM 'dags/temp/online_retail_origin.csv' DELIMITER ',' CSV HEADER;##

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=_load_data,
    )
    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> upload_retail_origin >> load_data >> end
    