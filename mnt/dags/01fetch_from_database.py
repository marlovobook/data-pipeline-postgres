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



def download_from_s3(ds, data_interval_start):

    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/session_raw_data/{ds_str}/table_product_demand_{ds}.csv'
    
    # Download CSV file from S3
    #Temp file for Raw Data (table_product_demand) from S3
    local_path = 'dags/temp'
    #f'/temp/session/{{{{ds}}}}.csv'
    file_name = s3_hook.download_file(
        key=s3_key,
        bucket_name=s3_bucket,
        local_path=local_path,
        #preserve_file_name=True,
        )
    
    return file_name

def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

#session_folder = f"src/session/{{{{execution_date.strftime('%Y/%m')}}}}/table_product_demand_{{{{ds}}}}.csv"

### Transform product demand into material demand with full file
#def transform_product_to_material(ds, next_ds, data_interval_start): <---- for using jinja template
def transform_product_to_material(data_interval_start, ds):
    
    # Read CSV file using Pandas
    df = pd.read_csv('dags/temp/table_product_demand.csv', index_col=False)
   
    # Transform Product to Materials
    df['local_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'expensive' else (20*row['demand'] if row['product_name'] == 'cheap' else 10* row['demand']) , axis=1 )
    df['foreign_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'cheap' else (10*row['demand'] if row['product_name'] in ['medium','expensive'] else 0), axis=1)
    df['robusta'] = df.apply(lambda row: 0 if row['product_name'] in ['cheap', 'medium'] else 10*row['demand'], axis=1)
    #Agg
    df = df.groupby(['date', 'shop_id'], as_index=False).agg({'local_arabica': 'sum', 'foreign_arabica': 'sum', 'robusta': 'sum'})
    #melt coffee beans columns into raw_material rows
    df = pd.melt(df, id_vars=['date', 'shop_id'], var_name='raw_material', value_name='demand')
    #change demand (g) into (kg)
    df['demand_kg'] = df['demand'] / 1000
    df = df.drop(columns = ['demand'])
    
    # ------------------   if sort by date ------------------#

    # df = df.sort_values(['date'], ['shop_id'])
    # df = df.reset_index(drop=True)
    #df['date'] = df['date'].astype('dbdate') <--- gcp syntax, please check if you want to use (probably pd.to_datetime | df = pd.to_datetime(df['date']))

    # ------------------   if sort by date ------------------#

    # ------------- Perform query on the data <<--This will be on another dag -----------------#

    #df = df.query(f"date >= '{ds}' and date < '{next_ds}'")

    # ------------- Perform query on the data <<--This will be on another dag -----------------#

    
    # --------------- the code below upload transformed file to S3 ---------------------#
    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv'
    df.to_csv(query_result_csv, index=False)

    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook.load_file(
        filename=query_result_csv,
        key=f"src/session_transformed_data/{ds_str}/table_material_demand_{ds}.csv",
        bucket_name='datalake',
        replace=True
          )
    
def _download_file_from_datalake(ds, data_interval_start):

    # Download File from S3
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook = S3Hook(aws_conn_id="minio")
    file_name_material = s3_hook.download_file(
        key=f"src/session_transformed_data/{ds_str}/table_material_demand_{ds}.csv",
        bucket_name="datalake",
    )

    return file_name_material

#form local files to postgres

def  _load_data_into_data_warehouse(**context):
    #data_interval_start = context["data_interval_start"]
    postgres_hook = PostgresHook(postgres_conn_id="pg_container")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from Xcoms
    file_name_material = context["ti"].xcom_pull(task_ids="download_transformed_file_from_datalake", key="return_value")
    #ds_str = data_interval_start.strftime('%Y_%m')
    # Copy file to datawarehouse in this case is postgres
    postgres_hook.copy_expert(

        f"""
            COPY
                dbo.table_material_demand

            FROM STDIN DELIMITER ',' CSV HEADER
    
        """,
        file_name_material,
    

    )

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
   
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,  
    )

    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'table_product_demand.csv'
        }
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=transform_product_to_material,
    )

    download_transformed_file_from_datalake = PythonOperator(
        task_id="download_transformed_file_from_datalake",
        python_callable=_download_file_from_datalake
    )

    create_table_in_data_warehouse = PostgresOperator(
        task_id='create_table_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand (
                date DATE,
                shop_id VARCHAR(100),
                raw_material VARCHAR(100),
                demand_kg VARCHAR(100)
            )
        
        """,
        )
    
    load_data_into_data_warehouse  = PythonOperator(
        task_id="load_data_into_data_warehouse",
        python_callable=_load_data_into_data_warehouse
    )

    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> fetch_from_database >> task_download_from_s3 >> task_rename_file >> task_upload_to_s3 >> download_transformed_file_from_datalake >> create_table_in_data_warehouse >> load_data_into_data_warehouse >> end
    