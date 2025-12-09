
import os
import sys
from pyspark.sql import SparkSession

print(f"Python: {sys.executable}")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")

try:
    spark = SparkSession.builder \
        .appName("Debug") \
        .master("local[*]") \
        .getOrCreate()
    print("Spark Session created successfully")
    spark.stop()
except Exception as e:
    print(f"Error: {e}")
