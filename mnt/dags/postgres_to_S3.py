import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pandas as pd


def _fetch_from_postgres(ds_nodash, next_ds_nodash):
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='pg_container')

    #step 1 : query data form postgresq db
    conn = hook.get_conn()
    cursor = conn.cursor()
    # %s = string of current date
    cursor.execute("SELECT * FROM dbo.table_product_demand WHERE date >=  %s and date < %s", (ds_nodash, next_ds_nodash))
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

        # # Step 2: Transform data using pandas
        # df = pd.read_csv(csv_file_path)
        # df['local_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'expensive' else (20 * row['demand'] if row['product_name'] == 'cheap' else 10 * row['demand']), axis=1)
        # df['foreign_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'cheap' else (10 * row['demand'] if row['product_name'] in ['medium', 'expensive'] else 0), axis=1)
        # df['robusta'] = df.apply(lambda row: 0 if row['product_name'] in ['cheap', 'medium'] else 10 * row['demand'], axis=1)

        # # Step 3: Save transformed data as CSV
        # transformed_csv_file_path = f"dags/transformed_orders_{ds_nodash}.csv"  # Specify the desired path for the transformed file
        # df.to_csv(transformed_csv_file_path, index=False)
        # logging.info("Transformed orders data: %s", transformed_csv_file_path)    

    #step 2 : Store .csv file into S3

        s3_hook = S3Hook(aws_conn_id="minio")
        s3_hook.load_file(
            filename=f.name,
            key=f"orders/{ds_nodash}.csv",
            bucket_name="datalake",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)


