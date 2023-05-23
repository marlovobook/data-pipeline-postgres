# from airflow import DAG
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.python_operator import PythonOperator
# # from airflow.contrib.hooks.minio_hook import MinioHook
# from airflow.utils.dates import days_ago
# from airflow.operators.dummy import DummyOperator

# from datetime import datetime


# # Define the DAG
# dag = DAG(
#     dag_id='postgres_to_minio',
#     start_date=days_ago(1),
#     schedule_interval="@once",
# )

# def extract_data_from_postgres():
#     # Connect to PostgreSQL
#     postgres_hook = PostgresHook(postgres_conn_id='pg_container')
    
#     # Execute SQL query and fetch data
#     connection = postgres_hook.get_conn()
#     cursor = connection.cursor()
#     cursor.execute('SELECT * FROM table_material_demand where month(date) = {}') #<< today -1 usigng Jinja templating
#     extracted_data = cursor.fetchall()
    
#     return extracted_data

# def check_quilty(dsadas):
#     assert 


# # Define the tasks
# # extract_data = PythonOperator(
# #     task_id='extract_data_from_postgres',
# #     python_callable=extract_data_from_postgres,
# #     dag=dag
# # )

# # upload_data = PythonOperator(
# #     task_id='upload_data_to_minio',
# #     python_callable=upload_to_minio,
# #     provide_context=True,
# #     dag=dag
# # )

# extract_data = DummyOperator(
#         task_id='extract_data_from_postgres_save_to_worker',
#         dag=dag
# )

# load_to_lake = DummyOperator(
#         task_id='load_data_to_lake',
#         dag=dag
# )

# start = DummyOperator(
#         task_id='start',
#         dag=dag
# )

# end = DummyOperator(
#         task_id='end',
#         dag=dag
# )




# # Set task dependencies
# start >> extract_data >> load_to_lake >> end
