
import os
import sys
from pyspark.sql import SparkSession

print(f"Python: {sys.executable}")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")

# Use a dummy URI just to test the connector loading
MONGO_URI = "mongodb://admin:mongodb_password@mongodb:27017/job_db.raw_jobs"

try:
    print("Attempting to create SparkSession with MongoDB connector 10.2.1...")
    spark = SparkSession.builder \
        .appName("DebugMongo") \
        .master("local[*]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.input.uri", MONGO_URI) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .getOrCreate()
    print("Spark Session created successfully with Mongo Connector")
    spark.stop()
except Exception as e:
    print(f"Error: {e}")
