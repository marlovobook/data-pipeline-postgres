
import logging
import csv
from datetime import datetime, timedelta

##### airflow ####
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
    

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup' : False
}
s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'

#query = f"SELECT * FROM dbo.table_product_demand WHERE date >= '{{{{ds}}}}' AND date < '{{{{next_ds_nodash}}}}'"
#year =
s3_folder = f"session/{{{{execution_date.strftime('%Y/%m')}}}}/table_product_demand_{{{{ds}}}}.csv"

s3_folder_material = f"session_material/{{{{execution_date.strftime('%Y/%m')}}}}/table_material_demand_{{{{ds}}}}.csv"

table_demand_sql = f"""

WITH raw_material AS (
  SELECT
    shop_id,
    date,
    CASE
      WHEN product_name = 'cheap' THEN CAST(demand AS NUMERIC) * 20
      WHEN product_name = 'medium' THEN CAST(demand AS NUMERIC) * 10
      ELSE 0
    END AS local_arabica,
    CASE
      WHEN product_name = 'cheap' THEN 0
      WHEN product_name = 'medium' THEN CAST(demand AS NUMERIC) * 10
      ELSE CAST(demand AS NUMERIC) * 10
    END AS foreign_arabica,
    CASE
      WHEN product_name = 'cheap' THEN 0
      WHEN product_name = 'medium' THEN 0
      ELSE CAST(demand AS NUMERIC) * 10
    END AS robusta
  FROM dbo.table_product_demand
  WHERE date >= '{{{{ds}}}}' AND date < '{{{{next_ds_nodash}}}}'
)

SELECT 
  date,
  shop_id,
  material_name,
  SUM(quantity) AS quantity_g
FROM (
  SELECT
    shop_id,
    date,
    'local_arabica' AS material_name,
    local_arabica AS quantity
  FROM raw_material
  UNION ALL
  SELECT
    shop_id,
    date,
    'foreign_arabica' AS material_name,
    foreign_arabica AS quantity
  FROM raw_material
  UNION ALL
  SELECT
    shop_id,
    date,
    'robusta' AS material_name,
    robusta AS quantity
  FROM raw_material
) AS unpivoted
GROUP BY
  date,
  shop_id,
  material_name
ORDER BY
  date,
  shop_id,
  material_name;

"""

def _download_file_from_datalake(**context):
    ds = context["ds"]

    # Download File from S3

    s3_hook = S3Hook(aws_conn_id="minio")
    file_name = s3_hook.download_file(
        key=s3_folder_material,
        bucket_name="datalake",
        replace=True,
    )

    return file_name

def  _load_data_into_data_warehouse(**context):
    
    postgres_hook = PostgresHook(postgres_conn_id="pg_container")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from Xcoms
    file_name = context["ti"].xcom_pull(task_ids="download_file_from_data_lake", key="return_value")

    # Copy file to datawarehouse in this case is postgres
    postgres_hook.copy_expert(

        """
            COPY
                create_table_in_data_warehouse_import

            FROM STDIN DELIMITER ',' CSV
    
        """,
        file_name,

    )

    

with DAG(
    'coffee_pipeline',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
    start_date=datetime(2023, 4, 18),  # Set the start date of the DAG
    #end_date=days_ago(1)
    
)as dags:
    
    start = DummyOperator(task_id="start")

    sql_to_s3_task = SqlToS3Operator(
        task_id="sql_to_s3_task",
        sql_conn_id='pg_container',
        query=f"SELECT * FROM dbo.table_product_demand WHERE date >= '{{{{ds}}}}' AND date < '{{{{next_ds_nodash}}}}'",
        #"SELECT * FROM dbo.table_product_demand WHERE date >= {{ds_nodash}} and date < {{next_ds_nodash}}",
        aws_conn_id="minio",
        s3_bucket='datalake',
        #s3_key=f"src_test/table_product_demand_{{{{ds}}}}.csv",
        s3_key=s3_folder,
        file_format="csv",
        pd_kwargs={"index": False},
        replace=True,
    )

    sql_to_s3_task_material = SqlToS3Operator(
        task_id="sql_to_s3_task_material",
        sql_conn_id='pg_container',
        query=table_demand_sql,
        aws_conn_id="minio",
        s3_bucket='datalake',
        #s3_key=f"src_test/table_product_demand_{{{{ds}}}}.csv",
        s3_key=s3_folder_material,
        file_format="csv",
        pd_kwargs={"index": False},
        replace=True,
    )

    #Temp table for imported data
    create_table_in_data_warehouse_import = PostgresOperator(
        task_id='create_table_in_data_warehouse_import',
        postgres_conn_id="pg_container",
        sql="""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand_import (
                date TIMESTAMP,
                shop_id VARCHAR(100),
                material_name VARCHAR(100),
                quantity_g VARCHAR(100)
            )
        
        """,
        )
  
    load_data_into_data_warehouse  = PythonOperator(
        task_id="load_data_into_data_warehouse",
        python_callable=_load_data_into_data_warehouse
    )

    #final table used for analysis
    create_final_table = PostgresOperator(
        task_id="create_final_tale",
        postgres_conn_id="pg_container",
        sql="""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand (
                date TIMESTAMP,
                shop_id VARCHAR(100),
                material_name VARCHAR(100),
                quantity_g VARCHAR(100)
            )
        
        """,

    )

    merge_import_into_final_table = PostgresOperator(
        task_id='merge_import_into_final_table',
        postgres_conn_id='pg_container',
        sql="""
            INSERT INTO dbo.table_material_demand (
            date,
            shop_id,
            material_name,
            quantity_g,
            
        )
        SELECT
            date,
            shop_id,
            material_name,
            quantity_g,
        FROM
            dbo.table_material_demand_import
        ON CONFLICT (timestamp)
        DO UPDATE SET
            date = EXCLUDED.date,
            shop_id = EXCLUDED.shop_id,
            material_name = EXCLUDED.material_name,
            quantity_g = EXCLUDED.quantity_g,
            
    """,
        
        )
    
    clear_import_table = PostgresOperator(
    task_id="clear_import_table",
    postgres_conn_id="pg_container",
    sql="""
        DELETE FROM dbo.table_material_demand
    """,
)
   

    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> sql_to_s3_task >> sql_to_s3_task_material  >> create_table_in_data_warehouse_import >> load_data_into_data_warehouse >> create_final_table >> merge_import_into_final_table >> clear_import_table >> end
