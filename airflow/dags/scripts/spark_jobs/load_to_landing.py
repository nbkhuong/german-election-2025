import os
from dotenv import load_dotenv
import boto3
import ast
from io import StringIO
import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

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


def main():
    # Establish spark session
    spark = SparkSession.builder \
                        .appName("Spark Basics") \
                        .getOrCreate()
    
    json_filenames = list(map(lambda x: x + ".json", DATA_ENTRIES))
    dfs = [] # list to store the dataframes

    # Load parliaments data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[1])
    data = response['Body'].read()
    json_data = data.decode('utf-8') 
    json_data = json.loads(json_data)
    rdd = spark.sparkContext.parallelize(json_data.items())
    df_parliaments = rdd.map(lambda x: (x[0], x[1]['Name'], x[1]['Shortcut'], x[1]['Election'])).toDF(["parliament_id", "parliament_name", "parliament_shortcut", "parliament_election"])
    df_parliaments.show()
    dfs.append(df_parliaments)

    # Load institutes data                
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[2])
    data = response['Body'].read()
    json_data = data.decode('utf-8') 
    json_data = json.loads(json_data)
    rdd = spark.sparkContext.parallelize(json_data.items())
    df_institutes = rdd.map(lambda x: (x[0], x[1]["Name"])).toDF(["institute_id", "institute_name"])
    df_institutes.show()
    dfs.append(df_institutes)

    # Load taskers data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[3])
    data = response['Body'].read()
    json_data = data.decode('utf-8') 
    json_data = json.loads(json_data)
    rdd = spark.sparkContext.parallelize(json_data.items())
    df_taskers = rdd.map(lambda x: (x[0], x[1]["Name"])).toDF(["tasker_id", "tasker_name"])
    df_taskers.show()
    dfs.append(df_taskers)

    # Load methods data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[4])
    data = response['Body'].read()
    json_data = data.decode('utf-8') 
    json_data = json.loads(json_data)
    rdd = spark.sparkContext.parallelize(json_data.items())
    df_methods = rdd.map(lambda x: (x[0], x[1]["Name"])).toDF(["method_id", "method_name"])
    df_methods.show()
    dfs.append(df_methods)

    # Load parties data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[5])
    data = response['Body'].read()
    json_data = data.decode('utf-8') 
    json_data = json.loads(json_data)
    rdd = spark.sparkContext.parallelize(json_data.items())
    df_parties = rdd.map(lambda x: (x[0], x[1]["Name"], x[1]["Shortcut"])).toDF(["party_id", "party_name", "party_shortcut"])
    df_parties.show()
    dfs.append(df_parties)

    # Load surveys data
    response = S3_CLIENT.get_object(Bucket="election-data", 
                                    Key="landing/" + json_filenames[6])
    data = response['Body'].read()
    json_data = data.decode('utf-8') 
    json_data = json.loads(json_data)
    rdd = spark.sparkContext.parallelize(json_data.items())
    df_surveys = rdd.map(lambda x: (x[0], 
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
    dfs.append(df_surveys)

    # Write the data to the trusted zone
    csv_filenames = list(map(lambda x: x + ".csv", DATA_ENTRIES))
    csv_buffer = StringIO()
    for i, df in enumerate(dfs): 
        if i != 0:
            df = df.toPandas()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()  
            S3_CLIENT.put_object(Bucket="election-data", 
                                Key="trusted/" + csv_filenames[i], 
                                Body=csv_content.encode('utf-8'),
                                ContentType="text/csv")
            
    spark.stop()


if __name__ == "__main__":
    main()
