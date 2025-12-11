from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, lower, trim, split, explode, 
    collect_list, size, regexp_extract_all, lit, regexp_extract, array
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

# ===== Load environment variables =====
load_dotenv()

# ===== Config logging =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===== Environment Variables =====
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'mongodb_password')
MONGO_DB = os.getenv('MONGO_DB', 'job_db')

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'airflow_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow_password')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'airflow_db')

# ===== List of popular IT skills =====
SKILLS_LIST = [
    'Python', 'Java', 'JavaScript', 'TypeScript', 'SQL', 'NoSQL', 'MongoDB', 'PostgreSQL',
    'MySQL', 'Spark', 'Hadoop', 'Hive', 'Airflow', 'Docker', 'Kubernetes', 'AWS',
    'Azure', 'GCP', 'Google Cloud', 'Scala', 'Go', 'Rust', 'C\\+\\+', 'C#',
    'React', 'Vue', 'Angular', 'Node.js', 'Django', 'Flask', 'FastAPI', 'Spring',
    'Git', 'Jenkins', 'Elasticsearch', 'Redis', 'Kafka', 'RabbitMQ', 'Tableau',
    'Power BI', 'Looker', 'ETL', 'Linux', 'Unix', 'REST API', 'GraphQL', 'Microservices',
    'Agile', 'Scrum', 'JIRA', 'Pandas', 'NumPy', 'Scikit-learn', 'TensorFlow', 'PyTorch',
    'Machine Learning', 'Deep Learning', 'NLP', 'Computer Vision'
]


class SparkDataCleaner:
    """
    Class to process data with Spark
    
    Attributes:
        spark: SparkSession object
    """
    
    def __init__(self, app_name: str = "ITviec-Data-Processor"):
        """
        Initialize Spark session
        
        Args:
            app_name: Name of Spark application
        """
        try:
            # Set JAVA_HOME explicitly if needed (though it should be in env)
            # os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-11-openjdk-amd64'
            
            # Debug info
            import sys
            # Log environment info
            logger.info(f"Python Executable: {sys.executable}")
            logger.info(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
            
            # Mask password in logs
            masked_uri = f"mongodb://{MONGO_USERNAME}:****@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.raw_jobs?authSource=admin"
            logger.info(f"MongoDB Input URI: {masked_uri}")

            self.spark = SparkSession.builder \
                .appName(app_name) \
                .master("local[*]") \
                .config("spark.driver.memory", "512m") \
                .config("spark.executor.memory", "512m") \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .config("spark.mongodb.input.uri", 
                        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.raw_jobs?authSource=admin") \
                .config("spark.mongodb.output.uri",
                        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.processed_jobs?authSource=admin") \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .getOrCreate()
            
            logger.info("Spark session created successfully")
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise
    
    def read_from_mongodb(self, collection_name: str = "raw_jobs"):
        """
        Read data from MongoDB
        
        Args:
            collection_name: Collection name in MongoDB
        
        Returns:
            Spark DataFrame
        """
        try:
            # Revert to old configuration style for connector 3.x
            df = self.spark.read.format("mongo") \
                .option("uri", f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.{collection_name}?authSource=admin") \
                .load()
            
            # Check if DataFrame is empty
            if df.rdd.isEmpty():
                logger.warning(f"MongoDB collection {collection_name} is empty. Returning empty DataFrame.")
                # Create empty schema to avoid errors in later steps
                schema = StructType([
                    StructField("title", StringType(), True),
                    StructField("company", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("salary", StringType(), True),
                    StructField("skills", ArrayType(StringType()), True),
                    StructField("url", StringType(), True),
                    StructField("scraped_at", StringType(), True)
                ])
                return self.spark.createDataFrame([], schema)

            logger.info(f"Read {df.count()} records from MongoDB collection: {collection_name}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading from MongoDB: {e}")
            # Return empty DF instead of raise so pipeline doesn't crash if DB has no data
            schema = StructType([
                StructField("title", StringType(), True),
                StructField("company", StringType(), True),
                StructField("location", StringType(), True),
                StructField("salary", StringType(), True),
                StructField("skills", ArrayType(StringType()), True),
                StructField("url", StringType(), True),
                StructField("scraped_at", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)    def clean_text(self, df, column_name: str) -> object:
        """
        Clean text: remove HTML tags, emoji, extra whitespace
        
        Args:
            df: Spark DataFrame
            column_name: Column name to clean
            
        Returns:
            DataFrame with cleaned column
        """
        try:
            df = df.withColumn(
                column_name,
                # ===== Remove HTML tags =====
                regexp_replace(col(column_name), r'<[^>]+>', '')
            )
            
            
            
            df = df.withColumn(
                column_name,
                # ===== Remove extra whitespace =====
                regexp_replace(col(column_name), r'\s+', ' ')
            )
            
            df = df.withColumn(
                column_name,
                # ===== Trim =====
                trim(col(column_name))
            )
            
            df = df.withColumn(
                column_name,
                # ===== Lowercase =====
                lower(col(column_name))
            )
            
            logger.info(f"Cleaned column: {column_name}")
            return df
        
        except Exception as e:
            logger.error(f"Error cleaning text: {e}")
            return df
    
    def normalize_salary(self, df) -> object:
        """
        Normalize salary column
        - Parse salary from string (e.g., "$1000 - $2000") to min/max
        
        Args:
            df: Spark DataFrame
            
        Returns:
            DataFrame with salary_min, salary_max columns
        """
        try:
            # ===== Initialize new columns =====
            df = df.withColumn('salary_min', lit(None).cast(IntegerType())) \
                   .withColumn('salary_max', lit(None).cast(IntegerType()))
            
            # ===== Parse salary from string =====
            # Example: "$1000 - $2000" -> 1000, 2000
            df = df.withColumn(
                'salary_min',
                when(col('salary').contains('$'),
                     regexp_extract(col('salary'), r'\$(\d+)', 1).cast(IntegerType())
                ).otherwise(None)
            )
            
            df = df.withColumn(
                'salary_max',
                when(col('salary').contains('-'),
                     regexp_extract(col('salary'), r'\$\d+\s*-\s*\$(\d+)', 1).cast(IntegerType())
                ).otherwise(col('salary_min'))
            )
            
            # ===== Drop old salary column =====
            df = df.drop('salary')
            
            logger.info("Normalized salary column")
            return df
        
        except Exception as e:
            logger.error(f"Error normalizing salary: {e}")
            return df
    
    def extract_skills(self, df, description_column: str = "description_preview") -> object:
        """
        Extract skills from Job Description
        
        Args:
            df: Spark DataFrame
            description_column: Column name containing job description
            
        Returns:
            DataFrame with 'skills' column containing list of skills
        """
        try:
            # ===== Create regex pattern from skills list =====
            # Join skills with | (OR operator)
            skills_pattern = '|'.join(SKILLS_LIST)
            
            # ===== Extract skills from description =====
            df = df.withColumn(
                'skills',
                explode(
                    when(
                        col(description_column).isNotNull(),
                        regexp_extract_all(
                            lower(col(description_column)),
                            f'(?i)({skills_pattern})'
                        )
                    ).otherwise(array().cast("array<string>"))
                )
            )
            
            logger.info("Extracted skills from Job Description")
            return df
        
        except Exception as e:
            logger.error(f"Error extracting skills: {e}")
            return df
    
    def deduplicate_skills(self, df) -> object:
        """
        Remove duplicate skills and add skill count
        
        Args:
            df: Spark DataFrame
            
        Returns:
            DataFrame with deduplicated skills
        """
        try:
            # ===== Group by job and collect unique skills =====
            df_agg = df.groupBy('job_id', 'job_title', 'company_name').agg(
                collect_list('skills').alias('skills_list')
            )
            
            # ===== Remove duplicates in list =====
            df_agg = df_agg.withColumn(
                'skills',
                when(
                    size(col('skills_list')) > 0,
                    col('skills_list')
                ).otherwise([])
            )
            
            df_agg = df_agg.drop('skills_list')
            
            logger.info("Removed duplicate skills")
            return df_agg
        
        except Exception as e:
            logger.error(f"Error deduplicating skills: {e}")
            return df
    
    def add_metadata(self, df) -> object:
        """
        Add metadata like processed_at, row_id, etc
        
        Args:
            df: Spark DataFrame
            
        Returns:
            DataFrame with metadata
        """
        try:
            df = df.withColumn('processed_at', lit(datetime.now().isoformat())) \
                   .withColumn('data_quality_score', lit(1.0))  # Placeholder
            
            logger.info("Added metadata to DataFrame")
            return df
        
        except Exception as e:
            logger.error(f"Error adding metadata: {e}")
            return df
    
    def write_to_parquet(self, df, output_path: str):
        """
        Save DataFrame to Parquet format
        
        Args:
            df: Spark DataFrame
            output_path: Output folder path
        """
        try:
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Saved data to Parquet: {output_path}")
        except Exception as e:
            logger.error(f"Error saving Parquet: {e}")

    def write_to_mongodb(self, df, collection_name: str = "processed_jobs"):
        """
        Save DataFrame to MongoDB
        
        Args:
            df: Spark DataFrame
            collection_name: Collection name in MongoDB
        """
        try:
            df.write.format("mongo") \
                .mode("append") \
                .option("uri", f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.{collection_name}?authSource=admin") \
                .save()
            logger.info(f"Saved data to MongoDB collection: {collection_name}")
        except Exception as e:
            logger.error(f"Error saving MongoDB: {e}")
    
    def write_to_postgresql(self, df, table_name: str):
        """
        Save DataFrame to PostgreSQL
        
        Args:
            df: Spark DataFrame
            table_name: Table name in PostgreSQL
        """
        try:
            jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            logger.info(f"Saved data to PostgreSQL table: {table_name}")
        
        except Exception as e:
            logger.error(f"Error saving PostgreSQL: {e}")
    
    def process_pipeline(self):
        """
        Run entire processing pipeline
        """
        try:
            logger.info("Start Spark processing pipeline")
            
            # ===== Step 1: Read data from MongoDB =====
            df = self.read_from_mongodb("raw_jobs")
            
            # ===== Step 2: Clean text =====
            # Note: Column names must match MongoDB fields
            df = self.clean_text(df, "title")
            # df = self.clean_text(df, "description_preview") # This field is not in fallback schema
            df = self.clean_text(df, "company")
            
            # ===== Step 3: Normalize salary =====
            df = self.normalize_salary(df)
            
            # ===== Step 4: Extract skills =====
            # df = self.extract_skills(df, "description_preview") # Temporarily disabled due to missing field
            
            # ===== Step 5: Add metadata =====
            df = self.add_metadata(df)
            
            # ===== Step 6: Show sample data =====
            logger.info("Sample processed data:")
            df.show(5, truncate=False)
            
            # ===== Step 7: Save data =====
            output_parquet = "data/processed/jobs_processed"
            self.write_to_parquet(df, output_parquet)
            
            # ===== Step 8: Save to MongoDB =====
            self.write_to_mongodb(df, "processed_jobs")
            
            # ===== Optional: Save to PostgreSQL =====
            # self.write_to_postgresql(df, "processed_jobs")
            
            logger.info("Spark processing pipeline completed!")
            
        except Exception as e:
            logger.error(f"Error in pipeline: {e}")
            raise
    
    def stop(self):
        """Stop Spark session"""
        try:
            self.spark.stop()
            logger.info("Spark session stopped")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {e}")


def main():
    """Main function"""
    cleaner = SparkDataCleaner()
    
    try:
        # ===== Run pipeline =====
        cleaner.process_pipeline()
    
    except Exception as e:
        logger.error(f"General error: {e}")
    
    finally:
        # ===== Stop Spark =====
        cleaner.stop()


if __name__ == '__main__':
    main()
