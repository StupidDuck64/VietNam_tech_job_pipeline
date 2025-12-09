
import os
import sys
from pyspark.sql import SparkSession

print(f"Python: {sys.executable}")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")

MONGO_URI = "mongodb://admin:mongodb_password@mongodb:27017/job_db.raw_jobs"

try:
    print("Attempting to create SparkSession with MongoDB connector...")
    spark = SparkSession.builder \
        .appName("DebugMongo") \
        .master("local[*]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.0.0") \
        .config("spark.mongodb.input.uri", MONGO_URI) \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    print("Spark Session created successfully with Mongo Connector")
    spark.stop()
except Exception as e:
    print(f"Error: {e}")
