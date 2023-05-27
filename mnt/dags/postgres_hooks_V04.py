import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

default_args = {
    'owner': 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def _fetch_from_postgres(ds_nodash, next_ds_nodash):
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='pg_container')

    # Step 1: Query data from PostgreSQL
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.table_product_demand WHERE date >=  %s and date < %s", (ds_nodash, next_ds_nodash))
    data = cursor.fetchall()

    # Download file to local
    csv_file_path = f"dags/get_orders_{ds_nodash}.csv"  # Specify the desired path for the exported file
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(data)
        f.flush()
    # Close the database connection
        cursor.close()
        conn.close()
        logging.info("Saved demand data: %s", csv_file_path)
         
    #step 2 : Store .csv file into S3
        s3_hook = S3Hook(aws_conn_id="minio")
        s3_hook.load_file(
            filename=f.name,
            key=f"orders/{ds_nodash}.csv",
            bucket_name="datalake",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)

def _transform_data(ds_nodash, **kwargs):
    
    s3_key = f"orders/{ds_nodash}.csv"  # Specify the S3 key of the downloaded file
    s3_bucket = "datalake"  # Specify the S3 bucket name
    s3_hook = S3Hook(aws_conn_id="minio")
    #csv_file_path = s3_hook.read_key(key=s3_key, bucket_name=s3_bucket)

    # # Step 2: Transform data using pandas
    csv_file_path = f"{ds_nodash}.csv"  # Specify the path of the downloaded file
    df = pd.read_csv(csv_file_path)
    # df['local_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'expensive' else (20 * row['demand'] if row['product_name'] == 'cheap' else 10 * row['demand']), axis=1)
    # df['foreign_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'cheap' else (10 * row['demand'] if row['product_name'] in ['medium', 'expensive'] else 0), axis=1)
    # df['robusta'] = df.apply(lambda row: 0 if row['product_name'] in ['cheap', 'medium'] else 10 * row['demand'], axis=1)

    # # Step 3: Save transformed data as CSV
    # transformed_csv_file_path = f"dags/transformed_orders_{ds_nodash}.csv"  # Specify the desired path for the transformed file
    # df.to_csv(transformed_csv_file_path, index=False)
    # logging.info("Transformed orders data: %s", transformed_csv_file_path)

    # # Step 4: Upload transformed CSV file to MinIO
    # #transformed_s3_key = f"orders/transformed_orders_{ds_nodash}.csv"  # Specify the S3 key for the transformed file
    # # Step 4: Upload transformed CSV file to MinIO
    # s3_hook.load_file(
    #     filename=transformed_csv_file_path,
    #     key=f"orders/transformed_orders_{ds_nodash}.csv",
    #     bucket_name="datalake",
    #     replace=True
    # )
    logging.info(f"Transformed orders {s3_key} file has been pushed to S3! %s {csv_file_path}")

with DAG(
        dag_id='hook_postgres_to_minio_V04',
        default_args=default_args,
        start_date=datetime(2023, 5, 20),
        schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id='fetch_from_postgres',
        python_callable=_fetch_from_postgres
    )

    task2 = PythonOperator(
        task_id='transform_data',
        python_callable=_transform_data,
        provide_context=True

    )




    task1 >> task2
