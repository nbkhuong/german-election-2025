from datetime import datetime, timedelta
import pandas as pd
import os
import json
import boto3
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator   

from german_election_2025.airflow.dags.scripts.util.retrieve_data import DataRetrieverOverAPI

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

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

# Define the DAG
dag = DAG(
    "germany_election_2025",
    default_args=default_args,
    description='ETL for German Election Umfrage',
    schedule=timedelta(days=1),
)

# Define the tasks
def extract(**kwargs):
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

    return 0

def load(**kwargs):

    return 0

def transform(**kwargs):

    return 0
    

# Create the tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# Define the task dependencies
extract_task >> load_task >> transform_task
