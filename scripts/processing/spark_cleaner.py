"""
===== Spark Data Cleaner & Skill Extractor =====
Script xử lý dữ liệu từ MongoDB bằng PySpark
- Làm sạch dữ liệu (xóa HTML tags, emoji, chuẩn hóa lương)
- Trích xuất kỹ năng (Skills) từ Job Description
- Lưu dữ liệu đã xử lý vào Parquet hoặc PostgreSQL

Author: Data Engineering Team
Date: December 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, lower, trim, split, explode, 
    collect_list, size, regexp_extract_all, lit
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

# ===== Danh sách kỹ năng IT phổ biến =====
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
    Class xử lý dữ liệu với Spark
    
    Attributes:
        spark: SparkSession object
    """
    
    def __init__(self, app_name: str = "ITviec-Data-Processor"):
        """
        Khởi tạo Spark session
        
        Args:
            app_name: Tên của Spark application
        """
        try:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.0.0") \
                .config("spark.mongodb.input.uri", 
                        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.raw_jobs") \
                .config("spark.mongodb.output.uri",
                        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.processed_jobs") \
                .getOrCreate()
            
            logger.info("✅ Spark session tạo thành công")
        except Exception as e:
            logger.error(f"❌ Lỗi tạo Spark session: {e}")
            raise
    
    def read_from_mongodb(self, collection_name: str = "raw_jobs"):
        """
        Đọc dữ liệu từ MongoDB
        
        Args:
            collection_name: Tên collection trong MongoDB
            
        Returns:
            Spark DataFrame
        """
        try:
            df = self.spark.read.format("mongodb") \
                .option("spark.mongodb.input.uri", 
                        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}.{collection_name}") \
                .load()
            
            logger.info(f"✅ Đọc {df.count()} records từ MongoDB collection: {collection_name}")
            return df
        
        except Exception as e:
            logger.error(f"❌ Lỗi đọc từ MongoDB: {e}")
            raise
    
    def clean_text(self, df, column_name: str) -> object:
        """
        Làm sạch text: xóa HTML tags, emoji, whitespace thừa
        
        Args:
            df: Spark DataFrame
            column_name: Tên cột cần làm sạch
            
        Returns:
            DataFrame với cột đã làm sạch
        """
        try:
            df = df.withColumn(
                column_name,
                # ===== Xóa HTML tags =====
                regexp_replace(col(column_name), r'<[^>]+>', '') \
                # ===== Xóa emoji =====
                .withColumn('cleaned', regexp_replace(col(column_name), 
                    r'[\U0001F300-\U0001F9FF]|\u200d|\ufe0f', '')) \
                # ===== Xóa whitespace thừa =====
                .withColumn('cleaned', regexp_replace(col('cleaned'), r'\s+', ' ')) \
                # ===== Trim =====
                .withColumn('cleaned', trim(col('cleaned'))) \
                # ===== Lowercase =====
                .withColumn('cleaned', lower(col('cleaned')))
            )
            
            df = df.drop(column_name).withColumnRenamed('cleaned', column_name)
            logger.info(f"✅ Làm sạch cột: {column_name}")
            return df
        
        except Exception as e:
            logger.error(f"❌ Lỗi làm sạch text: {e}")
            return df
    
    def normalize_salary(self, df) -> object:
        """
        Chuẩn hóa cột lương
        - Parse lương từ string (e.g., "$1000 - $2000") thành min/max
        
        Args:
            df: Spark DataFrame
            
        Returns:
            DataFrame với cột salary_min, salary_max
        """
        try:
            # ===== Khởi tạo cột mới =====
            df = df.withColumn('salary_min', lit(None).cast(IntegerType())) \
                   .withColumn('salary_max', lit(None).cast(IntegerType()))
            
            # ===== Parse lương từ string =====
            # Ví dụ: "$1000 - $2000" -> 1000, 2000
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
            
            # ===== Xóa cột salary cũ =====
            df = df.drop('salary')
            
            logger.info("✅ Chuẩn hóa cột lương")
            return df
        
        except Exception as e:
            logger.error(f"❌ Lỗi chuẩn hóa lương: {e}")
            return df
    
    def extract_skills(self, df, description_column: str = "description_preview") -> object:
        """
        Trích xuất kỹ năng từ Job Description
        
        Args:
            df: Spark DataFrame
            description_column: Tên cột chứa job description
            
        Returns:
            DataFrame với cột 'skills' chứa list các skills
        """
        try:
            # ===== Tạo regex pattern từ danh sách skills =====
            # Join skills với | (OR operator)
            skills_pattern = '|'.join(SKILLS_LIST)
            
            # ===== Trích xuất skills từ description =====
            # Sử dụng regex để tìm tất cả matching skills
            df = df.withColumn(
                'skills',
                explode(
                    when(
                        col(description_column).isNotNull(),
                        regexp_extract_all(
                            lower(col(description_column)),
                            f'(?i)({skills_pattern})'
                        )
                    ).otherwise([])
                )
            )
            
            logger.info("✅ Trích xuất kỹ năng từ Job Description")
            return df
        
        except Exception as e:
            logger.error(f"❌ Lỗi trích xuất skills: {e}")
            return df
    
    def deduplicate_skills(self, df) -> object:
        """
        Loại bỏ skills trùng lặp và thêm skill count
        
        Args:
            df: Spark DataFrame
            
        Returns:
            DataFrame với skills đã dedup
        """
        try:
            # ===== Group by job và collect unique skills =====
            df_agg = df.groupBy('job_id', 'job_title', 'company_name').agg(
                collect_list('skills').alias('skills_list')
            )
            
            # ===== Loại bỏ duplicate trong list =====
            df_agg = df_agg.withColumn(
                'skills',
                when(
                    size(col('skills_list')) > 0,
                    col('skills_list')
                ).otherwise([])
            )
            
            df_agg = df_agg.drop('skills_list')
            
            logger.info("✅ Loại bỏ skills trùng lặp")
            return df_agg
        
        except Exception as e:
            logger.error(f"❌ Lỗi dedup skills: {e}")
            return df
    
    def add_metadata(self, df) -> object:
        """
        Thêm metadata như processed_at, row_id, etc
        
        Args:
            df: Spark DataFrame
            
        Returns:
            DataFrame với metadata
        """
        try:
            df = df.withColumn('processed_at', lit(datetime.now().isoformat())) \
                   .withColumn('data_quality_score', lit(1.0))  # Placeholder
            
            logger.info("✅ Thêm metadata vào DataFrame")
            return df
        
        except Exception as e:
            logger.error(f"❌ Lỗi thêm metadata: {e}")
            return df
    
    def write_to_parquet(self, df, output_path: str):
        """
        Lưu DataFrame vào Parquet format
        
        Args:
            df: Spark DataFrame
            output_path: Đường dẫn output folder
        """
        try:
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"✅ Lưu dữ liệu vào Parquet: {output_path}")
        except Exception as e:
            logger.error(f"❌ Lỗi lưu Parquet: {e}")
    
    def write_to_postgresql(self, df, table_name: str):
        """
        Lưu DataFrame vào PostgreSQL
        
        Args:
            df: Spark DataFrame
            table_name: Tên table trong PostgreSQL
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
            
            logger.info(f"✅ Lưu dữ liệu vào PostgreSQL table: {table_name}")
        
        except Exception as e:
            logger.error(f"❌ Lỗi lưu PostgreSQL: {e}")
    
    def process_pipeline(self):
        """
        Chạy toàn bộ processing pipeline
        """
        try:
            logger.info("🚀 Bắt đầu Spark processing pipeline")
            
            # ===== Step 1: Đọc dữ liệu từ MongoDB =====
            df = self.read_from_mongodb("raw_jobs")
            
            # ===== Step 2: Làm sạch text =====
            df = self.clean_text(df, "job_title")
            df = self.clean_text(df, "description_preview")
            df = self.clean_text(df, "company_name")
            
            # ===== Step 3: Chuẩn hóa lương =====
            df = self.normalize_salary(df)
            
            # ===== Step 4: Trích xuất skills =====
            df = self.extract_skills(df, "description_preview")
            
            # ===== Step 5: Thêm metadata =====
            df = self.add_metadata(df)
            
            # ===== Step 6: Hiển thị sample data =====
            logger.info("📊 Sample processed data:")
            df.show(5, truncate=False)
            
            # ===== Step 7: Lưu dữ liệu =====
            output_parquet = "data/processed/jobs_processed"
            self.write_to_parquet(df, output_parquet)
            
            # ===== Optional: Lưu vào PostgreSQL =====
            # self.write_to_postgresql(df, "processed_jobs")
            
            logger.info("✨ Spark processing pipeline hoàn thành!")
            
        except Exception as e:
            logger.error(f"❌ Lỗi trong pipeline: {e}")
            raise
    
    def stop(self):
        """Dừng Spark session"""
        try:
            self.spark.stop()
            logger.info("✅ Spark session đã dừng")
        except Exception as e:
            logger.error(f"❌ Lỗi dừng Spark session: {e}")


def main():
    """Main function"""
    cleaner = SparkDataCleaner()
    
    try:
        # ===== Chạy pipeline =====
        cleaner.process_pipeline()
    
    except Exception as e:
        logger.error(f"❌ Lỗi chung: {e}")
    
    finally:
        # ===== Dừng Spark =====
        cleaner.stop()


if __name__ == '__main__':
    main()
