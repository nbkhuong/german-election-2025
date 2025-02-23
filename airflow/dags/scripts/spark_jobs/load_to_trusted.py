import os
from dotenv import load_dotenv
import boto3
import ast
from io import StringIO
import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Load the environment variables
load_dotenv()


# Read the environment variables
DAWUM_API_URL = os.getenv("DAWUM_API_URL")
MINIO_AWS_ACCESS_KEY_ID = os.getenv("MINIO_AWS_ACCESS_KEY_ID")
MINIO_AWS_SECRET_ACCESS_KEY = os.getenv("MINIO_AWS_SECRET_ACCESS_KEY")
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID")
DATA_ENTRIES = ast.literal_eval(os.getenv("DATA_ENTRIES"))
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID")

# Establish connection to Minio
S3_CLIENT = boto3.client('s3', 
                         endpoint_url=MINIO_ENDPOINT_URL, 
                         aws_access_key_id=MINIO_AWS_ACCESS_KEY_ID, 
                         aws_secret_access_key=MINIO_AWS_SECRET_ACCESS_KEY)


def write_to_minio_bucket(df, csv_buffer, csv_filename):
    df = df.toPandas()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()  
    S3_CLIENT.put_object(Bucket="election-data", 
                        Key="trusted/" + csv_filename, 
                        Body=csv_content.encode('utf-8'),
                        ContentType="text/csv")

def main():
    # Establish spark session
    spark = SparkSession.builder \
                        .appName("Spark Basics") \
                        .getOrCreate()
    
    json_filenames = list(map(lambda x: x + ".json", DATA_ENTRIES))

    # Load parliaments data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[1])
    data_parliaments = response['Body'].read()
    json_data_data_parliaments = json.loads(data_parliaments.decode('utf-8'))
    rdd_json_data_data_parliaments = spark.sparkContext.parallelize(json_data_data_parliaments.items())
    df_parliaments = rdd_json_data_data_parliaments.map(lambda x: (x[0], x[1]['Name'], x[1]['Shortcut'], x[1]['Election'])).toDF(["parliament_id", "parliament_name", "parliament_shortcut", "parliament_election"])
    df_parliaments.show()
    write_to_minio_bucket(df_parliaments, StringIO(), "parliaments.csv")

    # Load institutes data                
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[2])
    data_institutes = response['Body'].read()
    json_data_institutes = json.loads(data_institutes.decode('utf-8'))
    rdd_institutes = spark.sparkContext.parallelize(json_data_institutes.items())
    df_institutes = rdd_institutes.map(lambda x: (x[0], x[1]["Name"])).toDF(["institute_id", "institute_name"])
    df_institutes.show()
    write_to_minio_bucket(df_institutes, StringIO(), "institutes.csv")

    # Load taskers data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[3])
    data_taskers = response['Body'].read()
    json_data_taskers = json.loads(data_taskers.decode('utf-8'))
    rdd_taskers = spark.sparkContext.parallelize(json_data_taskers.items())
    df_taskers = rdd_taskers.map(lambda x: (x[0], x[1]["Name"])).toDF(["tasker_id", "tasker_name"])
    df_taskers.show()
    write_to_minio_bucket(df_taskers, StringIO(), "taskers.csv")

    # Load methods data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[4])
    data_methods = response['Body'].read()
    json_data_methods = json.loads(data_methods.decode('utf-8'))
    rdd_methods = spark.sparkContext.parallelize(json_data_methods.items())
    df_methods = rdd_methods.map(lambda x: (x[0], x[1]["Name"])).toDF(["method_id", "method_name"])
    df_methods.show()
    write_to_minio_bucket(df_methods, StringIO(), "methods.csv")

    # Load parties data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[5])
    data_parties = response['Body'].read()
    json_data_parties = json.loads(data_parties.decode('utf-8'))
    rdd_parties = spark.sparkContext.parallelize(json_data_parties.items())
    df_parties = rdd_parties.map(lambda x: (x[0], x[1]["Name"], x[1]["Shortcut"])).toDF(["party_id", "party_name", "party_shortcut"])
    df_parties.show()
    write_to_minio_bucket(df_parties, StringIO(), "parties.csv")

    # Load surveys data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + "surveys.json")
    data_surveys = response['Body'].read()
    json_data_surveys = json.loads(data_surveys.decode('utf-8'))
    rdd_surveys = spark.sparkContext.parallelize(json_data_surveys.items())
    df_surveys = rdd_surveys.map(lambda x: (x[0], 
                                    x[1]["Parliament_ID"],
                                    x[1]["Institute_ID"],
                                    x[1]["Tasker_ID"],
                                    x[1]["Method_ID"],
                                    x[1]["Date"], 
                                    x[1]["Survey_Period"]["Date_Start"], 
                                    x[1]["Survey_Period"]["Date_End"],
                                    x[1]["Surveyed_Persons"],
                                    x[1]["Results"])) \
                        .toDF(["survey_id", 
                               "parliament_id",
                               "institute_id",
                               "tasker_id",
                               "method_id",
                               "survey_publish_date",
                               "survey_start_date", 
                               "survey_end_date",
                               "total_surveyees",
                               "results"])
    df_surveys = df_surveys.select(F.col("survey_id"), 
                                F.col("parliament_id"),
                                F.col("institute_id"),
                                F.col("tasker_id"),
                                F.col("method_id"),
                                F.explode(df_surveys.results).alias("party_id", "survey_result_by_percent"),
                                F.col("survey_publish_date"),
                                F.col("survey_start_date"), 
                                F.col("survey_end_date"),
                                F.col("total_surveyees") 
                                )
    df_surveys = df_surveys.fillna({"survey_result_by_percent": 0})
    df_surveys.show()
    write_to_minio_bucket(df_surveys, StringIO(), "surveys.csv")
            
    spark.stop()


if __name__ == "__main__":
    main()
