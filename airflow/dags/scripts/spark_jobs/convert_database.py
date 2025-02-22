import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

def main():
    spark = SparkSession.builder \
                        .appName("Spark Basics") \
                        .getOrCreate()


    data = [("Alice", 25, "New York"), ("Bob", 30, "San Francisco"),
            ("Cathy", 22, "Chicago"), ("David", 35, "New York"),
            ("Eve", 29, "San Francisco")]
    columns = ["Name", "Age", "City"]

    df = spark.createDataFrame(data, columns)

    df.show()

    print("Filtering data")

    spark.stop()

if __name__ == "__main__":
    main()