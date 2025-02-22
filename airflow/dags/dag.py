from datetime import datetime, timedelta
import pandas as pd
import os
import json
import boto3
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator   

from pyspark import SparkContext
from pyspark.sql import SparkSession

from scripts.util.retrieve_data import DataRetrieverOverAPI

# Define the logger
LOGGER = logging.getLogger(__name__)

# Define the environment variables
DAWUM_API_URL = "https://api.dawum.de/"
MINIO_ENDPOINT_URL = "http://minio:9000/"
MINIO_AWS_ACCESS_KEY_ID = "vKNa0lEmBxjcO8l1EaL0"
MINIO_AWS_SECRET_ACCESS_KEY = "C95bTTeoaywJF95JwIUZhZaCA1xomN95BcgAeGUH"

# Establish connection to Minio
S3_CLIENT = boto3.client('s3', 
                         endpoint_url=MINIO_ENDPOINT_URL, 
                         aws_access_key_id=MINIO_AWS_ACCESS_KEY_ID, 
                         aws_secret_access_key=MINIO_AWS_SECRET_ACCESS_KEY
                        )
LOGGER.info("[DONE] - Connected to Minio")


with DAG(
    dag_id='germany_election_2025',
    start_date=datetime(2025, 2, 20),
    schedule=timedelta(days=1),
    description='Data Lakehouse for German Election Umfrage') as dag:


    @task
    def extract() -> None:
    # Extract data from api
        dawum_api = DataRetrieverOverAPI(DAWUM_API_URL)
        json_filenames = ['database.json', 
                        'parliaments.json', 
                        'institutes.json', 
                        'taskers.json', 
                        'methods.json', 
                        'parties.json', 
                        'surveys.json'
                        ]
        dawums = [dawum_api.data['Database'], 
                dawum_api.data['Parliaments'], 
                dawum_api.data['Institutes'], 
                dawum_api.data['Taskers'], 
                dawum_api.data['Methods'], 
                dawum_api.data['Parties'], 
                dawum_api.data['Surveys']
                ]
        
        LOGGER.info("[DONE] - Extracted data from API")

        # Write data to minio's bucket
        for i, file in enumerate(dawums):
            data = json.dumps(file, indent=4)   
            S3_CLIENT.put_object(Bucket="election-data", Key="landing/" + json_filenames[i], Body=data)

        LOGGER.info("[DONE] - Written data to Minio's bucket")


    def spark_job(task_id: str, 
                  spark_job_path: str, 
                  connection_id: str,
                  ) -> SparkSubmitOperator:
        
        return SparkSubmitOperator(
            task_id=task_id,
            application=spark_job_path,
            conn_id=connection_id, 
            verbose=True,
            #dag=dag
        )


    @task
    def load():

        return 0


    @task
    def transform():

        return 0
    
    extract_task = extract()

    spark_task = spark_job(task_id='spark_job_convert_database',  spark_job_path='/data/convert_database.py', connection_id='spark_conn')

    load_task = load()

    transform_task = transform()

    extract_task >> [load_task, spark_task] >> transform_task
    

# # Create the tasks
# extract_task = PythonOperator(
#     task_id='extract',
#     python_callable=extract,
#     dag=dag,
# )

# spark_job_convert_database = SparkSubmitOperator(
#         task_id='spark_job_convert_database',
#         application='/data/convert_database.py',
#         conn_id='spark_conn', 
#         verbose=True,
#         dag=dag
#     )

# transform_task = PythonOperator(
#     task_id='transform',
#     python_callable=transform,
#     dag=dag,
# )

# load_task = PythonOperator(
#     task_id='load',
#     python_callable=load,
#     dag=dag,
# )

# # Define the task dependencies
# extract_task >> [load_task, spark_job_convert_database] >> transform_task
