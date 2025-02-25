import os
from dotenv import load_dotenv
import boto3
import ast
from io import StringIO
import pandas as pd

from pyspark.sql import SparkSession

from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    spark = SparkSession.builder \
                        .appName("Spark Basics") \
                        .getOrCreate()
    
    DATA_ENTRIES.remove("database")
    csv_filenames = list(map(lambda x: x + ".csv", DATA_ENTRIES))
    dataframes = {}

    for csv_file in csv_filenames:

        response = S3_CLIENT.get_object(Bucket="election-data", Key="trusted/" + csv_file)
        
        csv_data = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_data))
        spark_df = spark.createDataFrame(df)
        dataframes[csv_file[:-4]] = spark_df
    

    for table_name, table_data in dataframes.items():
        dataframes[table_name].createOrReplaceTempView(table_name)
        spark.sql(f"select * from {table_name}").show()

    # Write data to curated zone
    survey_df = dataframes['surveys']
    parliaments_df = dataframes['parliaments']
    parties_df = dataframes['parties']
    survey_df = survey_df.join(parliaments_df, on="parliament_id", how="full").join(parties_df, on="party_id", how="full")

    unique_parliament_ids = parliaments_df.select("parliament_id").distinct().rdd.flatMap(lambda x: x).collect()
    unique_parliament_names = parliaments_df.select("parliament_name").distinct().rdd.flatMap(lambda x: x).collect()

    surveys_by_parliament = []
    for par_id in unique_parliament_ids:
        sur_df = survey_df.filter(survey_df.parliament_id == par_id)
        surveys_by_parliament.append(sur_df)

    for i, s in enumerate(surveys_by_parliament):
        S3_CLIENT.put_object(Bucket="election-data", Key="curated/" + unique_parliament_names[i] + ".json", Body=s.toPandas().to_json(orient="records"))

    # Write data to Postgres DB
    # Tables:  ['parliaments', 'institutes', 'taskers', 'methods', 'parties', 'surveys']
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW survey_result_by_party_temp AS
        SELECT 
            surveys.survey_result_by_percent, 
            surveys.survey_publish_date, 
            surveys.survey_start_date, 
            surveys.survey_end_date, 
            surveys.total_surveyees, 
            parties.party_name, 
            parties.party_shortcut,
            parliaments.parliament_shortcut
        FROM surveys
        FULL JOIN parties
        ON surveys.party_id = parties.party_id
        FULL JOIN parliaments
        ON surveys.parliament_id = parliaments.parliament_id
    """)
    result_df = spark.sql("SELECT * FROM survey_result_by_party_temp").toPandas()

    #spark.sql("SELECT * FROM survey_result_by_party_temp").show()

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    sql =   """
            CREATE SCHEMA IF NOT EXISTS election;
            DROP TABLE IF EXISTS election.survey_result_by_party;
            CREATE TABLE election.survey_result_by_party (
                survey_result_by_percent FLOAT,
                survey_publish_date DATE,
                survey_start_date DATE,
                survey_end_date DATE,
                total_surveyees INTEGER,
                party_name TEXT,
                party_shortcut TEXT,
                parliament_shortcut TEXT
            );
            """
    cursor.execute(sql)

    rows = [tuple(row) for row in result_df.itertuples(index=False)]
    insert_sql = """
        INSERT INTO election.survey_result_by_party (
            survey_result_by_percent, 
            survey_publish_date, 
            survey_start_date, 
            survey_end_date, 
            total_surveyees, 
            party_name, 
            party_shortcut,
            parliament_shortcut
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_sql, rows)

    conn.commit()
    conn.close()
    cursor.close()

    spark.stop()

    return 0


if __name__ == "__main__":
    main()
