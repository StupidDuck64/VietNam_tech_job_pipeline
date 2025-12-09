# VN IT Job Analytics - End-to-End Data Engineering Project

Dự án Data Engineering "End-to-End" để cào và phân tích dữ liệu việc làm IT từ ITviec.com.

## 📋 Tổng Quan

Luồng dữ liệu (ETL Pipeline):
1. **Ingestion**: Cào dữ liệu từ ITviec.com → Lưu vào MongoDB (JSON)
2. **Processing**: Đọc từ MongoDB → Làm sạch & trích xuất skill với Spark → Lưu Parquet
3. **Serving**: Ghi dữ liệu vào PostgreSQL Data Warehouse
4. **Orchestration**: Airflow điều phối quy trình hàng ngày

## 🗂️ Cấu Trúc Project

```
vn-it-job-analytics/
├── airflow/                   # Cấu hình Airflow
│   ├── dags/                  # DAG definitions
│   │   └── job_etl_dag.py
│   ├── logs/
│   └── plugins/
├── data/
│   ├── raw/                   # Dữ liệu JSON thô
│   └── processed/             # Dữ liệu Parquet đã xử lý
├── docker/                    # Docker configs
│   ├── airflow.Dockerfile
│   └── spark.Dockerfile
├── scripts/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── itviec_scraper.py
│   └── processing/
│       ├── __init__.py
│       └── spark_cleaner.py
├── sql/
│   ├── init_db.sql
│   └── queries.sql
├── docker-compose.yaml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

## 🚀 Hướng Dẫn Sử Dụng

### 1. Chuẩn Bị Môi Trường
```bash
# Clone project
git clone <repo-url>
cd vn-it-job-analytics

# Cài đặt Python packages
pip install -r requirements.txt
```

### 2. Khởi Động Infrastructure với Docker
```bash
docker-compose up -d
```

Các service sẽ chạy:
- **PostgreSQL**: `localhost:5432`
- **MongoDB**: `localhost:27017`
- **Airflow Webserver**: `http://localhost:8080`
- **Spark Master**: `http://localhost:8888` (nếu cấu hình)

### 3. Chạy Pipeline Thủ Công (Test)
```bash
# Chạy scraper
python scripts/ingestion/itviec_scraper.py

# Chạy Spark processing
spark-submit scripts/processing/spark_cleaner.py
```

### 4. Theo Dõi Airflow
- Mở browser: `http://localhost:8080`
- Username/Password: `airflow/airflow` (default)
- Trigger DAG `job_etl_dag` từ UI

## 🛠️ Công Nghệ Sử Dụng

- **Scraping**: Python, Requests, BeautifulSoup4
- **Database**: PostgreSQL (Data Warehouse), MongoDB (Raw Data)
- **Processing**: PySpark
- **Orchestration**: Apache Airflow
- **Containerization**: Docker, Docker Compose

## 📊 Tính Năng Chính

✅ Tự động cào dữ liệu việc làm từ ITviec.com
✅ Trích xuất kỹ năng (Skills) từ Job Description
✅ Làm sạch và chuẩn hóa dữ liệu (lương, vị trí, etc)
✅ Lưu trữ dữ liệu theo Star Schema (Fact + Dimension)
✅ Tự động hóa với Airflow scheduler (chạy hàng ngày 8 AM)
✅ Hỗ trợ phân tích và báo cáo

## 📝 Notes

- **Cần thay đổi**: Cập nhật `TARGET_URL` trong `.env` nếu muốn cào từ website khác
- **Data Privacy**: Hãy kiểm tra `robots.txt` và terms of service trước khi scrape
- **Performance**: Nếu RAM hạn chế, chỉnh lại `spark.executor.memory` trong config

## 🔗 Tham Khảo

- [Airflow Documentation](https://airflow.apache.org/)
- [PySpark Guide](https://spark.apache.org/docs/latest/api/python/)
- [MongoDB PyMongo](https://pymongo.readthedocs.io/)

---

**Created**: December 2025
**Status**: In Development ✨
