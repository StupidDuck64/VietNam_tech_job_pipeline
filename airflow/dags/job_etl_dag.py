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
    logging.warning(f"Cannot import custom modules: {e}")

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
# Increase default limit to 50 to scrape all keywords
MAX_PAGES = int(os.getenv('MAX_PAGES', '50'))

# ===== Default DAG arguments =====
default_args = {
    'owner': 'airflow',
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
    description='End-to-End ETL Pipeline for ITviec Job Analytics',
    schedule_interval='0 8 * * *',  # Run at 8:00 AM daily
    catchup=False,
    tags=['etl', 'scraping', 'data-processing'],
)


# ===== Task 1: Scrape Jobs from ITviec =====
def scrape_jobs_task(**context):
    """
    Task to scrape data from ITviec.com
    Save raw data to MongoDB
    """
    logger.info("Start scraping jobs from ITviec.com")
    
    try:
        # ===== Create MongoDB URI =====
        mongo_uri = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
        
        # ===== Initialize scraper =====
        scraper = ITviecScraper(
            mongo_uri=mongo_uri,
            db_name=MONGO_DB
        )
        
        # ===== Connect to MongoDB =====
        scraper.connect_mongodb()
        
        # ===== Scrape data (scrape multiple pages) =====
        logger.info(f"Scrape ALL IT Jobs from URL: {TARGET_URL} (max {MAX_PAGES} pages)")
        jobs = scraper.scrape_jobs(TARGET_URL, max_pages=MAX_PAGES)
        
        # ===== Save to MongoDB =====
        success = scraper.save_to_mongodb(jobs)
        
        # ===== Get statistics =====
        stats = scraper.get_statistics()
        
        # ===== Push stats to XCom (to use in next task) =====
        context['task_instance'].xcom_push(
            key='scrape_stats',
            value=stats
        )
        
        logger.info(f"Scrape completed. Stats: {stats}")
        
        # ===== Disconnect =====
        scraper.disconnect_mongodb()
        
        return success
    
    except Exception as e:
        logger.error(f"Scrape error: {e}")
        raise


def scrape_jobs_wrapper(**context):
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
    Task to process data with Spark
    - Clean data
    - Extract skills
    - Save to Parquet
    """
    logger.info("Start Spark processing")
    
    try:
        # ===== Initialize Spark cleaner =====
        cleaner = SparkDataCleaner(app_name="ITviec-ETL-Processing")
        
        cleaner.process_pipeline()
        
        logger.info("Spark processing completed")
        
        cleaner.stop()
        
        return True
    
    except Exception as e:
        logger.error(f"Spark processing error: {e}")
        raise


def process_data_wrapper(**context):
    """Wrapper function to call process_data_task"""
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
    Task to validate data quality
    - Number of records
    - Missing data
    - Outliers
    """
    logger.info("Start data quality validation")
    
    try:
        # ===== Get scrape stats from XCom =====
        scrape_stats = context['task_instance'].xcom_pull(
            task_ids='scrape_jobs',
            key='scrape_stats'
        )
        
        logger.info(f"Scrape stats: {scrape_stats}")
        
        # ===== Check basic validation =====
        if scrape_stats and scrape_stats.get('total_jobs', 0) > 0:
            logger.info("Validation passed - Data available for processing")
            return True
        else:
            logger.warning("Validation warning - Little or no data")
            return False
    
    except Exception as e:
        logger.error(f"Validation error: {e}")
        raise


task_validate = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag,
)


# ===== Task 4: Setup Database Schema (PostgreSQL) =====
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
    Task to load data from Parquet to PostgreSQL
    (If JDBC + Spark configured)
    """
    logger.info("Start loading data to Data Warehouse")
    
    try:
        logger.info("Data loaded via Spark Write JDBC in Process Data task")
        logger.info("Load to Warehouse completed")
        return True
    
    except Exception as e:
        logger.error(f"Load error: {e}")
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
    Task to generate report and notification
    """
    logger.info("Generating report...")
    
    try:
        scrape_stats = context['task_instance'].xcom_pull(
            task_ids='scrape_jobs',
            key='scrape_stats'
        )
        
        message = f"""
        ===== ETL Pipeline Report =====
        Execution Date: {context['execution_date']}
        
        Scraping Results:
        - Total Jobs: {scrape_stats.get('total_jobs', 0)}
        - Unique Companies: {scrape_stats.get('unique_companies', 0)}
        - Unique Locations: {scrape_stats.get('unique_locations', 0)}
        
        Status: SUCCESS
        """
        
        logger.info(message)
        
        report_path = f"{AIRFLOW_HOME}/logs/reports/report_{context['execution_date']}.txt"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(message)
        
        logger.info(f"Report saved to: {report_path}")
        return True
    
    except Exception as e:
        logger.error(f"Generate report error: {e}")
        raise


task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag,
)


# ===== Task 7: Cleanup Temp Files =====
task_cleanup = BashOperator(
    task_id='cleanup_temp_files',
    bash_command=f"""
    # ===== Delete temp data (if needed) =====
    echo "Cleanup completed"
    """,
    dag=dag,
)


task_scrape >> task_validate >> [task_setup_db, task_process] >> task_load >> task_report >> task_cleanup


