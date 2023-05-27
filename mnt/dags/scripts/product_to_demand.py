#!/usr/bin/env python3

import sys
# import csv
# import pandas as pd
# from airflow import DAG
import logging

# def _product_to_demand():
#     csv_file_path = f"table_product_demand_{{{{ds}}}}.csv"  # Specify the path of the downloaded file
#     df = pd.read_csv(csv_file_path)
#     df['local_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'expensive' else (20 * row['demand'] if row['product_name'] == 'cheap' else 10 * row['demand']), axis=1)
#     df['foreign_arabica'] = df.apply(lambda row: 0 if row['product_name'] == 'cheap' else (10 * row['demand'] if row['product_name'] in ['medium', 'expensive'] else 0), axis=1)
#     df['robusta'] = df.apply(lambda row: 0 if row['product_name'] in ['cheap', 'medium'] else 10 * row['demand'], axis=1)

#         # # Step 3: Save transformed data as CSV
#     #transformed_csv_file_path = f"dags/transformed_orders_{ds}.csv"  # Specify the desired path for the transformed file
#     #df.to_csv(transformed_csv_file_path, index=False)



logging.info("Orders file has been pushed to S3!")

sys.argv[1]
sys.argv[2]