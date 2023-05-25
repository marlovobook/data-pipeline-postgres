import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook





def copy_postgres_to_minio():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='pg_container')
    
    # Connect to MinIO (S3)
    s3_hook = S3Hook(aws_conn_id='minio')

    # Fetch data from PostgreSQL
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM dbo.table_product_demand')
    results = cursor.fetchall()

    # Prepare the data for uploading to MinIO
    # Here, we assume that the data is a CSV file.
    filename = 'data.csv'
    file_path = '/path/to/your/data.csv'  # Replace with the actual path to your data file
    s3_hook.load_file(
        filename=file_path,
        key=filename,
        bucket_name='your_minio_bucket',
        replace=True
    )

    # Close the connections
    cursor.close()
    conn.close()

def transform_data_in_s3():
    # Connect to MinIO (S3)
    s3_hook = S3Hook(aws_conn_id='s3_default')
    
    # Download the file from MinIO
    bucket_name = 'your_minio_bucket'
    filename = 'data.csv'
    file_path = '/tmp/data.csv'  # Replace with the desired local path for storing the downloaded file
    s3_hook.download_file(bucket_name, filename, file_path)

    # Perform the data transformation
    transformed_data = []
    with open(file_path, 'r') as file:
        for line in file:
            # Apply your transformation logic here
            transformed_line = line.strip() + ', additional column'  # Example transformation
            transformed_data.append(transformed_line)

    # Write the transformed data back to the file
    transformed_file_path = '/tmp/transformed_data.csv'  # Replace with the desired local path for storing the transformed file
    with open(transformed_file_path, 'w') as transformed_file:
        transformed_file.write('\n'.join(transformed_data))

    # Upload the transformed file to MinIO
    transformed_filename = 'transformed_data.csv'
    s3_hook.load_file(
        filename=transformed_file_path,
        key=transformed_filename,
        bucket_name=bucket_name,
        replace=True
    )

def upload_transformed_data_to_postgres():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Connect to MinIO (S3)
    s3_hook = S3Hook(aws_conn_id='s3_default')

    # Download the transformed file from MinIO
    bucket_name = 'your_minio_bucket'
    filename = 'transformed_data.csv'
    file_path = '/tmp/transformed_data.csv'  # Replace with the local path for storing the downloaded file
    s3_hook.download_file(bucket_name, filename, file_path)

    # Read the transformed data from the file
    transformed_data = []
    with open(file_path, 'r') as transformed_file:
        for line in transformed_file:
            transformed_data.append(line.strip().split(','))

    # Insert the transformed data into PostgreSQL
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for row in transformed_data:
        # Assuming the table has the same columns as the transformed data
        cursor.execute('INSERT INTO your_table VALUES (%s, %s, %s)', row)

    conn.commit()

    # Close the connections
    cursor.close()
    conn.close()



# Define your DAG
with DAG(
    'postgres_to_minio_transform',
    description='Copy file from PostgreSQL to MinIO, transform data in S3, and upload back to PostgreSQL',
    schedule_interval=None,  # Set your desired schedule interval
    start_date=datetime(2023, 5, 20),  # Set the start date of the DAG
)as dags:
    
        # Define the tasks
    copy_task = PythonOperator(
        task_id='copy_postgres_to_minio',
        python_callable=copy_postgres_to_minio,
    )

    transform_task = PythonOperator(
        task_id='transform_data_in_s3',
        python_callable=transform_data_in_s3,
       
    )

    upload_task = PythonOperator(
        task_id='upload_transformed_data_to_postgres',
        python_callable=upload_transformed_data_to_postgres,
    )

    # Set task dependencies
    copy_task >> transform_task >> upload_task

