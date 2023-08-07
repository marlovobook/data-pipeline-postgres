import os
import logging
import csv
import pandas as pd
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
#from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
#from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.sensors.external_task import ExternalTaskSensor
#from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator


s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'
s3_key='src/table_product_demand.csv'


def download_from_s3(ds, data_interval_start):

    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/session_raw_data/{ds_str}/table_product_demand_{ds}.csv'
    
    # Download CSV file from S3
    local_path = 'dags/temp'

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


"""
This def will transform product coluumn into material column
Then, upload back to S3
"""
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

    """
    this code will CREATE partition for each year and month in S3
    for example, Folder 2022, folder 2023
    and each year folder will have month folder: 01 02 03 to 12
    The file table_material_demand_2023-05-18.csv will be in
            datalake/src/session_transformed_data/2023/05/table_material_demand_2023-05-18.csv
    
    """
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv'
    df.to_csv(query_result_csv, index=False)

    #subtract date to yyyy folder and then MM folder
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

def  _load_data_into_data_warehouse(**context):
    
    postgres_hook = PostgresHook(postgres_conn_id="pg_container")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from Xcoms
    file_name_material = context["ti"].xcom_pull(task_ids="download_transformed_file_from_datalake", key="return_value")
    
    """
    The below lines of code could be used if want to create
    csv file with full month or full year instead of full day
    
    data_interval_start = context["data_interval_start"]
    ds_str = data_interval_start.strftime('%Y_%m')

    For example, 

    COPY
                dbo.table_material_demand_{ds_str}

                ===> dbo.table_material_demand_2022_05 
                (which will contain 1-month transaction)
    """
   
    # Copy file to datawarehouse in this case is postgres

    postgres_hook.copy_expert(

        f"""
            COPY
                dbo.table_material_demand

            FROM STDIN DELIMITER ',' CSV HEADER
    
        """,
        file_name_material,
    

    )

# Define your DAG ##
default_args = {
    'owner' : 'BOOK',
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    ######## w8 previous task ##########
    'wait_for_downstream' : True,
    'depends_on_past':True,
    ######## w8 previous task ##########
    'catchup' : False, 
}
with DAG(
    dag_id='02_datalake_to_datawarehouseV02',
    default_args=default_args,
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
    start_date=datetime(2023, 7, 25),  # Set the start date of the DAG

)as dags:
    
    ext_task_sensor = ExternalTaskSensor(
        
        #allowed_states will be ['success'] by default 
        #meaning this task will be success only the targeted task is in success stage
        task_id='check_product_demand_files',
        external_dag_id='01_database_to_datalake',
        external_task_id='fetch_from_database',
        # the worker wil poke to find the successful every 30s and stop working after 1800s
        timeout=1800,
        poke_interval=30,


        # Two mode:
        ## 'poke' = stand by mode while waiting for next poke
        ### 'reschedule' = sensor will terminate inself until the next poke
        mode='reschedule'
    )
    
    start = DummyOperator(task_id="start")
   
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,  
    )

    """
    table_product_demand (Datalake) -> tmp/@4rewr32552 when download to local
    so we have to rename it back to what it is
    because transform task will use pandas
    df = pd.read_csv('dags/temp/table_product_demand.csv', index_col=False)
    """
    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'table_product_demand.csv'
        }
    )

    task_upload_to_s3 = PythonOperator(
        task_id='transform_product_to_material_and_upload_to_s3',
        python_callable=transform_product_to_material,
    )

    download_transformed_file_from_datalake = PythonOperator(
        task_id="download_transformed_file_from_datalake",
        python_callable=_download_file_from_datalake
    )

    ### dbo.table_material_demand_2023_05
    ## can be 1 table per month if need be
    """
    dbo.table_material_demand_{{{{data_interval_start.strftime('%Y_%m')}}}}
    if today is 2023-02-15
    create table table_material_demand_2023_02
    in schema dbo
    ### the next dah will insert data of the today into this table
    """
    create_table_in_data_warehouse = PostgresOperator(
        task_id='create_table_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand (
                date DATE,
                shop_id VARCHAR(100),
                raw_material VARCHAR(100),
                demand_kg FLOAT
            )
        
        """,
        )
    
    load_data_into_data_warehouse  = PythonOperator(
        task_id="load_data_into_data_warehouse",
        python_callable=_load_data_into_data_warehouse
    )
    """
    Data Validation
    """
    data_validation_after_extract = SQLColumnCheckOperator(

            task_id='data_validation_after_extract',
            table='dbo.table_material_demand',
            column_mapping={
                "demand_kg" : {

                    #count that there are 0 row of null
                    "null_check" : {
                        "equal_to" : 0,
                        
                        #tolerance is a percentage 
                        #that the result may be out of bounds 
                        #but still considered successful.
                        # 0 = 0%; 1 = 100%
                        "tolerance" : 0,    
                    },

                    "min" : {
                        "geq_to" : 0
                    }
                }


            },
            # "geq_to" : -1012
            partition_clause=None,
            conn_id='pg_container',
            database=None,
            accept_none=True,

        )

    end = DummyOperator(task_id='end')

    ##download from datalake -> local for transforming -> back to S3 -> local -> postgres (as a datawarehouse)
    ext_task_sensor >> start  >> task_download_from_s3 >> task_rename_file >> task_upload_to_s3 >> download_transformed_file_from_datalake >> create_table_in_data_warehouse >> load_data_into_data_warehouse >> data_validation_after_extract >> end