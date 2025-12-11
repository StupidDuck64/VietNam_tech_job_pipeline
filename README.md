# Vietnam IT Job Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-blue)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-Raw%20Data-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-336791)

An end-to-end Data Engineering project that automates the collection, processing, and analysis of IT job market data in Vietnam. This pipeline ingests data from ITviec.com, processes it using Apache Spark, and loads it into a Data Warehouse for analytics.

## 🏗️ Architecture

The system follows a modern ETL architecture orchestrated by Apache Airflow:

1.  **Ingestion Layer**: A robust Python scraper (Selenium + BeautifulSoup) extracts job postings, handling anti-bot protections (Cloudflare) and dynamic content. Raw data is stored in **MongoDB**.
2.  **Processing Layer**: **Apache Spark** cleans, normalizes, and transforms the unstructured JSON data. It extracts key insights like skills, salaries, and locations.
3.  **Storage Layer**: Processed data is stored in **PostgreSQL** (Data Warehouse) for structured querying and **Parquet** files for historical archiving.
4.  **Orchestration**: **Apache Airflow** schedules and monitors the entire workflow daily.

For detailed architecture design, please refer to [ARCHITECTURE.md](ARCHITECTURE.md).

## ✨ Key Features

*   **Advanced Web Scraping**: Implements `selenium-stealth` and smart retry logic to bypass Cloudflare and CAPTCHA protections.
*   **Distributed Processing**: Utilizes PySpark for scalable data cleaning and transformation.
*   **Automated Workflow**: Fully containerized Airflow environment managing DAGs for daily data updates.
*   **Data Quality Checks**: Integrated validation steps to ensure data integrity before loading into the warehouse.
*   **Containerization**: Entire stack (Airflow, Spark, Databases) is defined in `docker-compose` for easy deployment.

## 🛠️ Tech Stack

*   **Language**: Python 3.9+
*   **Orchestration**: Apache Airflow
*   **Processing**: Apache Spark (PySpark)
*   **Databases**: MongoDB (NoSQL), PostgreSQL (Relational)
*   **Infrastructure**: Docker, Docker Compose
*   **Libraries**: Selenium, BeautifulSoup4, Pandas

## 🚀 Getting Started

### 📋 System Requirements
*   **OS**: Windows, macOS, or Linux.
*   **Docker Desktop**: v4.0+ (Ensure it is running).
*   **RAM**: Minimum 8GB (16GB recommended for Spark + Airflow).
*   **Disk Space**: At least 10GB free.

### 🔧 Step 1: Environment Setup
1.  **Install Docker Desktop**: Download from [docker.com](https://www.docker.com/products/docker-desktop).
2.  **Verify Installation**:
    ```bash
    docker --version
    docker-compose --version
    ```

### 📂 Step 2: Installation
1.  **Clone the repository**:
    ```bash
    git clone https://github.com/yourusername/vn-it-job-analytics.git
    cd vn-it-job-analytics
    ```
2.  **Check Project Structure**: Ensure you see folders like `airflow/`, `data/`, `docker/`, etc.
3.  **Environment Variables**: The project uses a `.env` file. If missing, create one or use the defaults in `docker-compose.yaml`.

### 🐳 Step 3: Start Infrastructure
1.  **Build and Start Containers**:
    ```bash
    docker-compose up -d --build
    ```
    *This process may take 5-10 minutes initially to build the custom Airflow and Spark images.*

2.  **Check Running Containers**:
    ```bash
    docker-compose ps
    ```
    Ensure `airflow-webserver`, `airflow-scheduler`, `spark-master`, `spark-worker`, `postgres`, and `mongodb` are `Up`.

### 🖥️ Step 4: Access Interfaces
*   **Airflow UI**: [http://localhost:8080](http://localhost:8080)
    *   **Username**: `airflow`
    *   **Password**: `airflow`
*   **Spark Master UI**: [http://localhost:8888](http://localhost:8888)
*   **Spark Worker UI**: [http://localhost:8081](http://localhost:8081)

### ⚡ Step 5: Run the Pipeline
1.  Access the **Airflow UI**.
2.  Locate the `job_etl_dag`.
3.  Toggle the **ON/OFF** switch to **ON**.
4.  Click the **Trigger DAG** (Play button) to start the pipeline manually.
5.  Monitor the task progress in the **Graph View**.

### ❓ Troubleshooting
*   **Port Conflicts**: If port 8080 is taken, change `AIRFLOW_PORT` in `.env` or `docker-compose.yaml`.
*   **Memory Issues**: If containers crash, increase Docker memory limit to 8GB+.

## 📂 Project Structure

```
vn-it-job-analytics/
├── airflow/                   # Airflow configuration & DAGs
│   ├── dags/                  # Workflow definitions
│   │   └── job_etl_dag.py
│   └── ...
├── data/                      # Data storage (mapped volumes)
│   ├── raw/                   # Raw JSON from MongoDB dump
│   └── processed/             # Parquet files
├── docker/                    # Dockerfiles for custom images
├── scripts/                   # Source code
│   ├── ingestion/             # Scraper logic
│   ├── processing/            # Spark ETL jobs
│   └── utils/                 # Helper scripts
├── sql/                       # Database initialization scripts
├── docker-compose.yaml        # Infrastructure definition
└── requirements.txt           # Python dependencies
```

## 📈 Future Improvements

*   Add visualization dashboard (Metabase/Superset).
*   Implement CI/CD pipeline for automated testing.
*   Expand data sources to other job boards (TopDev, VietnamWorks).

---
