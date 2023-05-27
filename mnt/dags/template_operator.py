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
        elif 'Stats' in event:
            stats = event['Stats']
            # Process stats if needed

    # Process the data further as per your requirements
    print(data)
      
    
    # Define the SQL query
    # 

def _dump():
    query = f"SELECT * FROM {file_name} WHERE date >= '{ds}' AND date < '{next_ds_nodash}'"
    logging.info("Orders file %s has been pushed to S3!", ds)
    logging.info("Orders file %s has been pushed to S3!", next_ds_nodash)
    logging.info("Orders file %s has been pushed to S3!", result)
        # Retrieve the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
        # Read the CSV data into a DataFrame
    df = pd.read_csv(response['Body']) 
        # Execute the query
    result = df.query(query)
    
    logging.info("query : %s", result)

def _transform_s3(ds_nodash, next_ds_nodash):
    # Connect to S3
    hook = S3Hook(aws_conn_id='minio')

    #step 1 : query data form S3 as datalake
    conn = hook.get_conn()
    cursor = conn.cursor()
    # 1st %s = string of the start date (ds_nodash), 2nd %s = string = string of todaydate as interval =@Daily
    cursor.execute("SELECT * FROM table_product_demand.csv WHERE date >=  %s and date < %s", (ds_nodash, next_ds_nodash))
    data = cursor.fetchall()



    #download file to local
    csv_file_path = f"dags/get_orders_{ds_nodash}.csv"  # Specify the desired path for the exported file
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    #with open(csv_file_path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(data)
    # flush file object so the .csv is saved
        f.flush()    
    # Close the database connection
        cursor.close()
        conn.close()
        logging.info("Saved demand data: %s", csv_file_path)

    #step 2 : Store .csv file into S3

        s3_hook = S3Hook(aws_conn_id="minio")
        s3_hook.load_file(
            filename=f.name,
            #key = new folder in bucket you want to save to
            key=f"orders/{ds_nodash}.csv",
            bucket_name="datalake",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)
        logging.info("Orders file %s has been pushed to S3!", ds_nodash)

# Define your DAG
with DAG(
    'test',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
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

    transform_s3 = PythonOperator(
        task_id='transform_s3',
        python_callable=_transform
    )


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
    start >> sql_to_s3_task  >> transform_s3 >> end
