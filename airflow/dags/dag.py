from datetime import datetime, timedelta
import os
import json
import boto3
import logging
from dotenv import load_dotenv
import ast

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

from scripts.util.retrieve_data import DataRetrieverOverAPI

# Define the logger
LOGGER = logging.getLogger(__name__)

# Establish environment variables
load_dotenv()

# Define the environment variables
DAWUM_API_URL = os.getenv("DAWUM_API_URL")
MINIO_AWS_ACCESS_KEY_ID = os.getenv("MINIO_AWS_ACCESS_KEY_ID")
MINIO_AWS_SECRET_ACCESS_KEY = os.getenv("MINIO_AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID")
DATA_ENTRIES = ast.literal_eval(Variable.get("DATA_ENTRIES", default_var="[]"))
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID")

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
    def extract():
        # Setup and fetch data entries to retrieve from the api
        dawum_api = DataRetrieverOverAPI(DAWUM_API_URL)
        json_filenames = list(map(lambda x: x + ".json", DATA_ENTRIES))
        data_entries = list(map(lambda x: x.capitalize(), DATA_ENTRIES))
    
        # Write data to minio's bucket
        for i, data_entry in enumerate(data_entries):
            data = json.dumps(dawum_api.data[data_entry], indent=4)
            S3_CLIENT.put_object(Bucket="election-data", Key="landing/" + json_filenames[i], Body=data)

        print(os.environ)
        LOGGER.info("[DONE] - Written data to Minio's bucket")


    def spark_job(task_id: str, 
                  spark_job_path: str, 
                  connection_id: str,
                  ) -> SparkSubmitOperator:
        
        return SparkSubmitOperator(
            task_id=task_id,
            application=spark_job_path,
            conn_id=connection_id, 
            verbose=True
        )


    @task
    def START():

        return ["PIPELINE STARTED"]
    

    @task
    def END():

        return ["PIPELINE ENDED"]
    

    # Define the tasks
    load = spark_job(task_id='load',  
                     spark_job_path='/data/load.py', 
                     connection_id=SPARK_CONN_ID)
    
    transform = spark_job(task_id='transform',
                          spark_job_path='/data/transform.py',
                          connection_id=SPARK_CONN_ID)

    # Execute the tasks
    START() >> extract() >> load >> transform >> END()
    