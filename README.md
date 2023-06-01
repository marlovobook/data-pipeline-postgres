![Alt text](images/pipeline-overview.png)
# step1 :
```bash
docker-compose build
```

# step2 :
```bash
docker-compose up -d
```

# delete container
```bash
docker-compose down -v
```

# check IP Address

```bash
docker inspect pg_container
```

Postgres (Database) [dag]>> sensors >> Transform in MinIO[dag]


## Full version readme on Medium :
------
Data Engineer Project: Postgres (Database)to S3 (Datalake) to Postgres (Datawarehouse)
This is my side project while I was an intern at DataCafe Thailand
TL;DR : ETL project : postgres >> S3 >> transform in local >> S3 >> download transformed file >> create table in data warehouse >> load data into data warehouse
Disclaimer: This is one of the first projects I have done, I am sure there are better ways to redo what I did - this is more like my personal journal
Feel free to give the advice!
 - - - - - - - - - - - - - - - - - - 

Table of Contents
1. Why do I do this
2. What I want to see
3. Tools and Methodology
4. Extract-Transform-load code


 1.- - - - - - - - - - - - - - - - - - - - -


# Why do I do this
This is the case scenario.
My customer is a coffee shop owner who has many branches and franchises all over the country.
Now the customer wants to know when they(they as a singular pronoun) should order the coffee bean (EOQ/ROP in the inventory management), which can be predicted using ML.
Further studies can be read in the links below

*****

However, I, as a Data Engineer, have to prepare the data for the Data Scientists.
…but How to do so? let's see in the next chapter


2. - - - - - - - - - - - - - - - - - - - - - 

# What I want to see

Basically transforming

++++++++++++++++++++++++

 'table_product_demand' 
into
 'table_material_demand'

++++++++++++++++++++++++

They are 3 types of products: 'cheap', 'medium', 'expensive'

They are 3 types of materials: 'local_arabica', 'foreign_arabica', 'robusta'

[unit: g]

'cheap': using 20 xlocal_arabica

'medium': using 10 x local_arabica + 10 xforeign_arabica

'expensive': using 10 x foreign_arabica + 10 x robusta



3. - - - - - - - - - - - - - - - - - - - - - 

# Tools and Methodology

to make it simple(is it?) and able to run locally(at no cost), I won't use anything cloudy (lol, is that a pun?)
Ahem, let's get back to the main story…
Docker - of course; to build, test, deploy
Postgres - as database and data warehouse
minio (S3)-as a datalake
airflow - of course, as a workflow management platform 

That's it!
…also, Github, to save the source code

```
I will use ETL method
```

Postgres (Database) to S3 (Datalake) to Postgres (Data Warehouse)
This is the rough flow; however, in detail, it will be like this:
@_@

1st dag -: 
```
fetch_from_database >> task_download_from_s3 >> task_rename_file >> task_upload_to_s3
```

2nd dag -: 

```
task_download_from_s3 >> task_rename_file >> upload_transfromed_files_to_s3 >> download_transformed_file_from_datalake >> create_table_in_data_warehouse >> load_data_into_data_warehouse
```

The reason I split them into 2 dags:

the 1st dag is about fetching raw table_product_demand which is very big and transforming it into table_material_demand could use some time
table_product_demand.csv -> table_material_demand.csv

the 2nd dag basically partitioning the transformed .csv by date

For example, 

table_material_demand_2023–04–20, 

table_material_demand_2023–04–21,

table_material_demand_2023–04–22,

…and so on

so it takes less time
```
df = df.query(f"date >= '{ds}' and date < '{next_ds}'")
```
I believed there are better way than split it into 2 dags, but I will stick with this method for now

 - - - - - 

Also, if you look into it they are lots of download_from_s3 (to_local) and task_rename (As for why renaming, I will explain in another chapter)
This is because I did it on my local computer, probably not like this on a cloud where operators and services are varied.

……

The code be explained in the next chapter for how to do this pipeline….

4. - - - - - - - - - - - - - - - - - - - - - 

# Extract-Transform-load code


All codes and docker-compose setup can be looked through at
https://github.com/marlovobook/data-pipeline-postgres/tree/main

1st Dag: database_to_datalake

```
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
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator

s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'
s3_key='src/table_product_demand.csv'

def download_from_s3():
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/table_product_demand.csv'
    
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

session_folder = f"src/session/{{{{execution_date.strftime('%Y/%m')}}}}/table_product_demand_{{{{ds}}}}.csv"

### Transform product demand into material demand with full file
#def transform_product_to_material(ds, next_ds, data_interval_start): <---- for using jinja template
def transform_product_to_material():
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

    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv'
    df.to_csv(query_result_csv, index=False)
    #ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook.load_file(
        filename=query_result_csv,
        key=f"src/table_material_demand.csv",
        #key=f"src/session/{ds.strftime('%Y/%m')}/table_product_demand_{ds}.csv",
        bucket_name='datalake',
        replace=True
          )

default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup' : False
}
# Define your DAG
with DAG(
    dag_id='01_database_to_datalake',
    default_args=default_args,
    description='Copy file from PostgreSQL(database) to MinIO(datalake), the transform and load will be in another dag file',
    schedule_interval=None,  # Set your desired schedule interval '@daily'
    start_date=datetime(2023, 5, 25),  # Set the start date of the DAG

)as dags:
    
    start = DummyOperator(task_id="start")

    fetch_from_database = SqlToS3Operator(
        task_id="fetch_from_database",
        sql_conn_id='pg_container',
        query='SELECT * FROM dbo.table_product_demand', #<<--- Basically copy everything
        aws_conn_id="minio",
        s3_bucket='datalake',
        s3_key=f"src/table_product_demand.csv",
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

    end = DummyOperator(task_id='end')

    # Set task dependencies
    start >> fetch_from_database >> task_download_from_s3 >> task_rename_file >> task_upload_to_s3  >> end
    #start  >> task_download_from_s3 >> task_rename_file >> task_upload_to_s3 >> end

```

Code Explain:

fetch_from_database

Fetch data from Postgres and Upload to S3 directly

Using SqlToS3Operator

with query 

```

'SELECT * FROM dbo.table_product_demand'

```


s3_key=f"src/table_product_demand.csv" use be used even not the MinioIO (Whatever that is S3)

```
fetch_from_database = SqlToS3Operator(
        task_id="fetch_from_database",
        sql_conn_id='pg_container',
        query='SELECT * FROM dbo.table_product_demand', #<<--- Basically copy everything
        aws_conn_id="minio",
        s3_bucket='datalake',
        s3_key=f"src/table_product_demand.csv", #<<--fetch into this on S3 Minio
        replace=True,
        file_format="csv",
        pd_kwargs={"index": False} #<<---- if True, they will be another column containing numbers
    )

task_download_from_s3 will call download_from_s3
task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,  
    )
```
Download from S3 to local

```
s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'
s3_key='src/table_product_demand.csv'

def download_from_s3():
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/table_product_demand.csv'
    
    # Download CSV file from S3
    #Temp file for Raw Data (table_product_demand) from S3
    local_path = 'dags/temp'
    file_name = s3_hook.download_file(
        key=s3_key,
        bucket_name=s3_bucket,
        local_path=local_path,
        )
    
    return file_name

```
However, the downloaded file name from S3Hook will be randomly generated, that's why we need to rename it


task_rename_file

```

task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'table_product_demand.csv'
        }
    )
def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

```


task_upload_to_s3

Finally, Transform and upload back to S3 as it is a datalake
Transform code and Upload will be in the same function

```
upload_transfromed_files_to_s3 = PythonOperator(
        task_id='upload_transfromed_files_to_s3',
        python_callable=transform_material_by_date,
    )

```
we will read the renamed table_product_demand.csv

### Transform product demand into material demand with full file

```

def transform_product_to_material():
    # Read CSV file using Pandas
    df = pd.read_csv('dags/temp/table_product_demand.csv', index_col=False)
   
    # Transform Product to Material
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

    # --------------- the code below upload transformed file to S3 ---------------------#
    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv'
    df.to_csv(query_result_csv, index=False)
    s3_hook.load_file(
        filename=query_result_csv,
        key=f"src/table_material_demand.csv",
        bucket_name='datalake',
        replace=True
          )
```


the result from the code

Note
    ```
    ds, next_ds, data_interval_start
    ```

are from https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
Airflow template which is based on Jinja Template

There is an alternative way using (**context)

```
def transform_material_by_date(**context):
    ds = context['ds']
    next_ds = context['next_ds']
    data_interval_start context['next_ds']
    # Read CSV file using Pandas
    ..........
    ......
    ....
    ..
    .
```

……………………Done for the 1st Dag……………………………………………

2nd Dags: datalake_to_datawarehouse

```
import os
import logging
import csv
import pandas as pd
import psycopg2
import boto3
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
#from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator
#from airflow.providers.amazon.aws.operators.athena import AthenaOperator

s3_hook = S3Hook(aws_conn_id='minio')
postgres_hook = PostgresHook(postgres_conn_id='pg_container')
bucket_name = 'datalake'
s3_key='src/table_product_demand.csv'

def download_from_s3():
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/table_material_demand.csv'
    
    # Download CSV file from S3
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

session_folder = f"src/session/{{{{execution_date.strftime('%Y/%m')}}}}/table_product_demand_{{{{ds}}}}.csv"

def transform_material_by_date(ds, next_ds, data_interval_start):
    # Read CSV file using Pandas
    df = pd.read_csv('dags/temp/table_material_demand.csv', index_col=False)
    # # Perform query on the data 
    df = df.query(f"date >= '{ds}' and date < '{next_ds}'")
    #df = pd.to_datetime(df['date'])
    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv'
    df.to_csv(query_result_csv, index=False)
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook.load_file(
        filename=query_result_csv,
        key=f"src/session/{ds_str}/table_material_demand_{ds}.csv",
        #key=f"src/session/{ds.strftime('%Y/%m')}/table_product_demand_{ds}.csv",
        bucket_name='datalake',
        replace=True
          )

def _download_file_from_datalake(ds, data_interval_start):

    # Download File from S3
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook = S3Hook(aws_conn_id="minio")
    file_name_material = s3_hook.download_file(
        key=f"src/session/{ds_str}/table_material_demand_{ds}.csv",
        bucket_name="datalake",
    )

    return file_name_material

def  _load_data_into_data_warehouse(**context):
    data_interval_start = context["data_interval_start"]
    postgres_hook = PostgresHook(postgres_conn_id="pg_container")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from Xcoms
    file_name_material = context["ti"].xcom_pull(task_ids="download_transformed_file_from_datalake", key="return_value")
    ds_str = data_interval_start.strftime('%Y_%m')
    # Copy file to datawarehouse in this case is postgres
    postgres_hook.copy_expert(

        f"""
            COPY
                dbo.table_material_demand_{ds_str}

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
    dag_id='02_datalake_to_datawarehouse',
    default_args=default_args,
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval='@daily',  # Set your desired schedule interval
    start_date=datetime(2023, 4, 20),  # Set the start date of the DAG
)as dags:
    
    start = DummyOperator(task_id="start")
   
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,  
    )

    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'table_material_demand.csv'
        }
    )

    upload_transfromed_files_to_s3 = PythonOperator(
        task_id='upload_transfromed_files_to_s3',
        python_callable=transform_material_by_date,
    )

    download_transformed_file_from_datalake = PythonOperator(
        task_id="download_transformed_file_from_datalake",
        python_callable=_download_file_from_datalake
    )

    ### dbo.table_material_demand_2023_05
    ## 1 table per month
    create_table_in_data_warehouse = PostgresOperator(
        task_id='create_table_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand_{{{{data_interval_start.strftime('%Y_%m')}}}} (
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

    ##download from datalake -> local for transforming -> postgres (as a datawarehouse)
    start  >> task_download_from_s3 >> task_rename_file >> upload_transfromed_files_to_s3 >> download_transformed_file_from_datalake

    download_transformed_file_from_datalake >> create_table_in_data_warehouse >> load_data_into_data_warehouse >> end

```

Nothings change much

task_download_from_s3

Download file from S3 (Datalake) to local

```
   task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,  
    )


def download_from_s3():
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_bucket ='datalake'
    s3_key =f'/src/table_material_demand.csv'
    
    # Download CSV file from S3
    local_path = 'dags/temp'
    file_name = s3_hook.download_file(
        key=s3_key,
        bucket_name=s3_bucket,
        local_path=local_path,
        #preserve_file_name=True,
        )
    
    return file_name

```

task_rename_file

As always, S3Hook randomly generates the name, so you need to rename but this time rename it to 'table_material_demand.csv'

```

def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 'table_material_demand.csv'
        }
    )
upload_transfromed_files_to_s3

```
def transform_material_by_date(ds, next_ds, data_interval_start):
    # Read CSV file using Pandas
    df = pd.read_csv('dags/temp/table_material_demand.csv', index_col=False)

    # # Perform query on the data 
    df = df.query(f"date >= '{ds}' and date < '{next_ds}'")

    # Upload query result back to S3
    query_result_csv = f'dags/result_csv/TEMP_FILE.csv' ###<<--- the next day file will replace this, so you won't have multiply .csv
    df.to_csv(query_result_csv, index=False)

    '''
    this code will CREATE partition for each year and month in S3
    for example, Folder 2022, folder 2023
    and each year folder will have month folder: 01 02 03 to 12
    The file table_material_demand_2023-05-18.csv will be in
            datalake/session/2023/05/table_material_demand_2023-05-18.csv
    
    '''
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook.load_file(
        filename=query_result_csv,
        key=f"src/session/{ds_str}/table_material_demand_{ds}.csv",
        bucket_name='datalake',
        replace=True
          )

    upload_transfromed_files_to_s3 = PythonOperator(
        task_id='upload_transfromed_files_to_s3',
        python_callable=transform_material_by_date,
    )
```
Now we have transformed the files into partitioned by day, it's time to load into Data Warehouse
TL;DL: download to local -> Create table on Data Warehouse > load the transformed file into Data Warehouse
download_transformed_file_from_datalake
    download_transformed_file_from_datalake = PythonOperator(
        task_id="download_transformed_file_from_datalake",
        python_callable=_download_file_from_datalake
    )

def _download_file_from_datalake(ds, data_interval_start):

    # Download File from S3
    ds_str = data_interval_start.strftime('%Y/%m')
    s3_hook = S3Hook(aws_conn_id="minio")
    file_name_material = s3_hook.download_file(
        key=f"src/session/{ds_str}/table_material_demand_{ds}.csv",
        bucket_name="datalake",
    )

    return file_name_material
```

this will save the file name on Xcoms

create_table_in_data_warehouse

I will create 1 Table per month (Which can be configured up to you)

```
    ### dbo.table_material_demand_2023_05
    ## 1 table per month
"""
    dbo.table_material_demand_{{{{data_interval_start.strftime('%Y_%m')}}}}
    if today is 2023-05-15
    create table table_material_demand_2023_05 in schema dbo in Postgres
    ### the next dah will insert data of the today into this table
"""
    create_table_in_data_warehouse = PostgresOperator(
        task_id='create_table_in_data_warehouse',
        postgres_conn_id="pg_container",
        sql=f"""
            CREATE TABLE IF NOT EXISTS dbo.table_material_demand_{{{{data_interval_start.strftime('%Y_%m')}}}} (
                date DATE,
                shop_id VARCHAR(100),
                raw_material VARCHAR(100),
                demand_kg VARCHAR(100)
            )
        
        """,
        )
```
load_data_into_data_warehouse

Finally, load data into the table we have just created

```

def  _load_data_into_data_warehouse(**context):
    data_interval_start = context["data_interval_start"]
    postgres_hook = PostgresHook(postgres_conn_id="pg_container")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from Xcoms
    file_name_material = context["ti"].xcom_pull(task_ids="download_transformed_file_from_datalake", key="return_value")
    ds_str = data_interval_start.strftime('%Y_%m')
    # Copy file to datawarehouse in this case is postgres
    postgres_hook.copy_expert(

        f"""
            COPY
                dbo.table_material_demand_{ds_str}

            FROM STDIN DELIMITER ',' CSV HEADER
    
        """,
        file_name_material,

    )

    load_data_into_data_warehouse  = PythonOperator(
        task_id="load_data_into_data_warehouse",
        python_callable=_load_data_into_data_warehouse
    )

```

That's it!

Now, Data Scientists can use this transformed table as material in the time-series forecast model