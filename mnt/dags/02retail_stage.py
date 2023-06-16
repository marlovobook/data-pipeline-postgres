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


#Create function for loading staged table into postgres

def _load_data_stage():

    postgres_hook = PostgresHook(postgres_conn_id='pg_container')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    file_name = 'dags/temp/online_retail_stage.csv'

    #Copy_expery needs to call file_name
    #STDIN means Standard Input = an input stream where data issent to and read by a program
    #In this case, it is file_name variable
    postgres_hook.copy_expert(

        """
            COPY 
                dbo.table_online_retail_stage(id, Invoice, StockCode, Description,Quantity,InvoiceDate,Price,Customer_ID,Country,last_updated, operation)
            
            FROM STDIN DELIMITER ',' CSV HEADER

        """,
        file_name,

    )




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
    dag_id='02_retail_stage',
    default_args=default_args,
    description='Copy online_retail_stage file from local',
    schedule_interval=None,  # Set your desired schedule interval '@daily'
    start_date=datetime(2023, 4, 25),  # Set the start date of the DAG

)as dags:
    
    start = DummyOperator(task_id="start")

    upload_retail_stage = PostgresOperator(
        task_id='create_online_retail_stage_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            DROP TABLE IF EXISTS dbo.table_online_retail_stage;

            CREATE TABLE dbo.table_online_retail_stage (
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
                operation char(1),
                constraint table_online_retail_stage_pk primary key (id, last_updated)
            );

            
        
        """,
    )

    #merge the changes from staging table into target table
    #possible to make in incremental load

    merge_changes_table = PostgresOperator(
        task_id = "merge_chages",
        postgres_conn_id="pg_container",
        sql =f"""

            merge into dbo.table_online_retail_origin
            using
            (
                SELECT distinct id,
                    first_value(invoice) over w as invoice, 
                    first_value(stockcode) over w as stockcode, 
                    first_value(description) over w as description, 
                    first_value(quantity) over w as quantity, 
                    first_value(invoicedate) over w as invoicedate, 
                    first_value(price) over w as price, 
                    first_value(customer_id) over w as customer_id, 
                    first_value(country) over w as country, 
                    first_value(last_updated) over w as last_updated, 
                    first_value(operation) over w as operation
                FROM dbo.table_online_retail_stage
                    window w as (partition by id order by last_updated desc)
                order by id

            ) cdc
            on dbo.table_online_retail_origin.id=cdc.id
            when not matched and cdc.operation='I' then
                insert values(cdc.id, cdc.invoice, cdc.stockcode, cdc.description, cdc.quantity, cdc.invoicedate, cdc.price, cdc.customer_id, cdc.country, cdc.last_updated)
            when matched and cdc.operation='D' then
                delete
            when matched and cdc.operation='U' then
                update set  invoice=cdc.invoice,
                            stockcode=cdc.stockcode,
                            description=cdc.description,
                            quantity=cdc.quantity,
                            invoicedate=cdc.invoicedate,
                            price=cdc.price,
                            customer_id=cdc.customer_id,
                            country=cdc.country,
                            last_updated=cdc.last_updated
            ;
;

        """
    )

    load_data = PythonOperator(
        task_id="load_data_stage",
        python_callable=_load_data_stage,
    )
    delete_temp_table = DummyOperator(task_id='delete_temp_table')

    

    # Set task dependencies
    start >> upload_retail_stage >> load_data >> merge_changes_table >> delete_temp_table
    