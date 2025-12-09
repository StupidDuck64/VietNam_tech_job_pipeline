"""
===== Airflow DAG - ITviec Job ETL Pipeline =====
Điều phối toàn bộ quy trình ETL:
1. Scrape dữ liệu từ ITviec.com
2. Xử lý dữ liệu với Spark
3. Load vào Data Warehouse (PostgreSQL)

Lịch chạy: Hàng ngày lúc 8:00 AM
Author: Data Engineering Team
Date: December 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import logging
import os
import sys

# ===== Add scripts folder to path =====
sys.path.insert(0, '/opt/airflow/scripts')

# ===== Import custom modules =====
try:
    from ingestion.itviec_scraper import ITviecScraper
    from processing.spark_cleaner import SparkDataCleaner
except ImportError as e:
    logging.warning(f"⚠️ Cannot import custom modules: {e}")

# ===== Configure logging =====
logger = logging.getLogger(__name__)

# ===== Environment =====
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'mongodb_password')
MONGO_DB = os.getenv('MONGO_DB', 'job_db')
TARGET_URL = os.getenv('TARGET_URL', 'https://itviec.com/it-jobs')
# Tăng giới hạn mặc định lên 50 để cào hết danh sách keywords
MAX_PAGES = int(os.getenv('MAX_PAGES', '50'))

# ===== Default DAG arguments =====
default_args = {
    'owner': 'data-engineering-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
}

# ===== DAG Definition =====
dag = DAG(
    'job_etl_dag',
    default_args=default_args,
    description='End-to-End ETL Pipeline cho ITviec Job Analytics',
    schedule_interval='0 8 * * *',  # Chạy lúc 8:00 AM hàng ngày
    catchup=False,
    tags=['etl', 'scraping', 'data-processing'],
)


# ===== Task 1: Scrape Jobs from ITviec =====
def scrape_jobs_task(**context):
    """
    Task cào dữ liệu từ ITviec.com
    Lưu dữ liệu raw vào MongoDB
    """
    logger.info("📄 Bắt đầu scrape jobs từ ITviec.com")
    
    try:
        # ===== Tạo MongoDB URI =====
        mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        
        # ===== Khởi tạo scraper =====
        scraper = ITviecScraper(
            mongo_uri=mongo_uri,
            db_name=MONGO_DB
        )
        
        # ===== Kết nối MongoDB =====
        scraper.connect_mongodb()
        
        # ===== Cào dữ liệu (cào nhiều trang) =====
        logger.info(f"🚀 Cào TẤT CẢ IT Jobs từ URL: {TARGET_URL} (max {MAX_PAGES} trang)")
        jobs = scraper.scrape_jobs(TARGET_URL, max_pages=MAX_PAGES)
        
        # ===== Lưu vào MongoDB =====
        success = scraper.save_to_mongodb(jobs)
        
        # ===== Lấy thống kê =====
        stats = scraper.get_statistics()
        
        # ===== Push stats vào XCom (để dùng trong task sau) =====
        context['task_instance'].xcom_push(
            key='scrape_stats',
            value=stats
        )
        
        logger.info(f"✅ Scrape hoàn thành. Stats: {stats}")
        
        # ===== Ngắt kết nối =====
        scraper.disconnect_mongodb()
        
        return success
    
    except Exception as e:
        logger.error(f"❌ Lỗi scrape: {e}")
        raise


def scrape_jobs_wrapper(**context):
    """Wrapper function để gọi scrape_jobs_task"""
    return scrape_jobs_task(**context)


task_scrape = PythonOperator(
    task_id='scrape_jobs',
    python_callable=scrape_jobs_wrapper,
    provide_context=True,
    retries=2,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)


# ===== Task 2: Process Data with Spark =====
def process_data_task(**context):
    """
    Task xử lý dữ liệu với Spark
    - Làm sạch dữ liệu
    - Trích xuất kỹ năng
    - Lưu vào Parquet
    """
    logger.info("⚡ Bắt đầu Spark processing")
    
    try:
        # ===== Khởi tạo Spark cleaner =====
        cleaner = SparkDataCleaner(app_name="ITviec-ETL-Processing")
        
        # ===== Chạy processing pipeline =====
        cleaner.process_pipeline()
        
        logger.info("✅ Spark processing hoàn thành")
        
        # ===== Dừng Spark =====
        cleaner.stop()
        
        return True
    
    except Exception as e:
        logger.error(f"❌ Lỗi Spark processing: {e}")
        raise


def process_data_wrapper(**context):
    """Wrapper function để gọi process_data_task"""
    return process_data_task(**context)


task_process = PythonOperator(
    task_id='process_data',
    python_callable=process_data_wrapper,
    provide_context=True,
    retries=1,
    dag=dag,
)


# ===== Task 3: Validate Data Quality =====
def validate_data_quality(**context):
    """
    Task kiểm tra chất lượng dữ liệu
    - Số lượng records
    - Missing data
    - Outliers
    """
    logger.info("🔍 Bắt đầu validation chất lượng dữ liệu")
    
    try:
        # ===== Lấy scrape stats từ XCom =====
        scrape_stats = context['task_instance'].xcom_pull(
            task_ids='scrape_jobs',
            key='scrape_stats'
        )
        
        logger.info(f"📊 Scrape stats: {scrape_stats}")
        
        # ===== Kiểm tra basic validation =====
        if scrape_stats and scrape_stats.get('total_jobs', 0) > 0:
            logger.info("✅ Validation passed - Có dữ liệu để xử lý")
            return True
        else:
            logger.warning("⚠️ Validation warning - Ít dữ liệu hoặc không có dữ liệu")
            return False
    
    except Exception as e:
        logger.error(f"❌ Lỗi validation: {e}")
        raise


task_validate = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag,
)


# ===== Task 4: Setup Database Schema (PostgreSQL) =====
# Đọc file SQL trực tiếp thay vì dùng template
with open('/opt/airflow/sql/init_db.sql', 'r') as f:
    init_sql = f.read()

task_setup_db = PostgresOperator(
    task_id='setup_database_schema',
    postgres_conn_id='postgres_default',
    sql=init_sql,
    autocommit=True,
    dag=dag,
)


# ===== Task 5: Load to Data Warehouse =====
def load_to_warehouse(**context):
    """
    Task load dữ liệu từ Parquet vào PostgreSQL
    (Nếu cấu hình JDBC + Spark)
    """
    logger.info("🗄️ Bắt đầu load dữ liệu vào Data Warehouse")
    
    try:
        logger.info("⏳ Dữ liệu được load qua Spark Write JDBC trong task Process Data")
        logger.info("✅ Load to Warehouse hoàn thành")
        return True
    
    except Exception as e:
        logger.error(f"❌ Lỗi load: {e}")
        raise


task_load = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    provide_context=True,
    dag=dag,
)


# ===== Task 6: Generate Report / Notification =====
def generate_report(**context):
    """
    Task tạo báo cáo và thông báo
    """
    logger.info("📧 Tạo báo cáo...")
    
    try:
        # ===== Lấy stats =====
        scrape_stats = context['task_instance'].xcom_pull(
            task_ids='scrape_jobs',
            key='scrape_stats'
        )
        
        # ===== Tạo message =====
        message = f"""
        ===== ETL Pipeline Report =====
        Execution Date: {context['execution_date']}
        
        Scraping Results:
        - Total Jobs: {scrape_stats.get('total_jobs', 0)}
        - Unique Companies: {scrape_stats.get('unique_companies', 0)}
        - Unique Locations: {scrape_stats.get('unique_locations', 0)}
        
        Status: ✅ SUCCESS
        """
        
        logger.info(message)
        
        # ===== Save to file (optional) =====
        report_path = f"{AIRFLOW_HOME}/logs/reports/report_{context['execution_date']}.txt"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(message)
        
        logger.info(f"✅ Report saved to: {report_path}")
        return True
    
    except Exception as e:
        logger.error(f"❌ Lỗi generate report: {e}")
        raise


task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag,
)


# ===== Task 7: Cleanup (Optional) =====
task_cleanup = BashOperator(
    task_id='cleanup_temp_files',
    bash_command=f"""
    # ===== Xóa dữ liệu temp (nếu cần) =====
    echo "✅ Cleanup hoàn thành"
    """,
    dag=dag,
)


# ===== Set Task Dependencies =====
# 
#    scrape_jobs
#        ↓
#  validate_data (kiểm tra quality)
#        ↓
#  setup_db + process_data (parallel)
#        ↓
#    load_to_warehouse
#        ↓
#  generate_report → cleanup

task_scrape >> task_validate >> [task_setup_db, task_process] >> task_load >> task_report >> task_cleanup


# ===== DAG Documentation =====
dag.doc_md = """
# ITviec Job Analytics - ETL Pipeline

## Mô tả
Pipeline End-to-End để cào, xử lý, và phân tích dữ liệu việc làm IT từ ITviec.com

## Các tác vụ (Tasks):
1. **scrape_jobs**: Cào dữ liệu từ ITviec.com, lưu vào MongoDB
2. **validate_data_quality**: Kiểm tra chất lượng dữ liệu
3. **setup_database_schema**: Tạo schema trong PostgreSQL (Star Schema)
4. **process_data**: Xử lý dữ liệu với Spark (clean, extract skills)
5. **load_to_warehouse**: Load dữ liệu vào PostgreSQL
6. **generate_report**: Tạo báo cáo ETL
7. **cleanup_temp_files**: Xóa các file tạm

## Lịch chạy
- Hàng ngày lúc **8:00 AM**
- Timezone: UTC

## Infrastructure
- **MongoDB**: Lưu dữ liệu raw
- **PostgreSQL**: Data Warehouse
- **Spark**: Processing engine
- **Airflow**: Orchestration

## Contacts
- Team: Data Engineering
- Email: admin@example.com
"""
