# 🚀 VN IT Job Analytics - Setup & Run Guide

**Hướng dẫn chi tiết từng bước để setup và chạy dự án Data Engineering End-to-End**

---

## 📋 Yêu cầu Hệ Thống

- **OS**: Windows, macOS, Linux
- **Docker Desktop**: v4.0+ (https://www.docker.com/products/docker-desktop)
- **Python**: 3.9+ (nếu chạy locally)
- **RAM tối thiểu**: 8GB (recommend 16GB cho Spark + Airflow)
- **Disk**: Ít nhất 10GB

---

## 🔧 Bước 1: Chuẩn Bị Môi Trường

### 1.1 Cài đặt Docker Desktop

1. **Windows/Mac**: 
   - Download từ https://www.docker.com/products/docker-desktop
   - Cài đặt như bình thường
   - Khởi động Docker Desktop

2. **Linux** (Ubuntu/Debian):
   ```bash
   sudo apt-get install docker.io docker-compose
   sudo usermod -aG docker $USER
   ```

### 1.2 Verify Docker Installation

```bash
docker --version
docker-compose --version
```

Nếu hiển thị version → ✅ Docker đã sẵn sàng

---

## 📂 Bước 2: Clone / Setup Project

### 2.1 Di chuyển vào project folder

```bash
cd c:\Users\c9283\PyCharmMiscProject\DE_project\DE_Project_01\vn-it-job-analytics
```

### 2.2 Verify folder structure

```bash
# Windows PowerShell
dir

# Linux/Mac
ls -la
```

Bạn sẽ thấy:
```
├── airflow/
├── data/
├── docker/
├── scripts/
├── sql/
├── docker-compose.yaml
├── requirements.txt
├── .env
├── .gitignore
├── README.md
└── SETUP_GUIDE.md (file này)
```

### 2.3 Kiểm tra .env file

File `.env` phải chứa các biến môi trường. Nếu không có, tạo file mới:

```bash
# Xem nội dung (Linux/Mac)
cat .env

# Xem nội dung (Windows PowerShell)
Get-Content .env
```

---

## 🐳 Bước 3: Khởi Động Infrastructure

### 3.1 Build Docker Images

```bash
# Di chuyển vào project folder (nếu chưa)
cd vn-it-job-analytics

# Build tất cả images
docker-compose build
```

⏳ **Quá trình này sẽ mất 5-10 phút** (tùy vào internet speed)

### 3.2 Khởi động tất cả Services

```bash
# Khởi động tất cả container ở background
docker-compose up -d
```

✅ Lệnh sẽ:
- Khởi động PostgreSQL
- Khởi động MongoDB  
- Khởi động Airflow Webserver & Scheduler
- Khởi động Spark Master & Worker

### 3.3 Kiểm tra Status

```bash
# Liệt kê tất cả running containers
docker-compose ps
```

Bạn sẽ thấy output như:

```
NAME                    STATUS
postgres_db             Up 2 minutes
mongodb_service         Up 2 minutes
airflow_webserver       Up 1 minute
airflow_scheduler       Up 1 minute
spark_master            Up 1 minute
spark_worker            Up 1 minute
```

### 3.4 Kiểm tra Logs (Debug)

Nếu có container bị "Exited", kiểm tra logs:

```bash
# Xem logs của Airflow Webserver
docker-compose logs airflow-webserver

# Xem logs của MongoDB
docker-compose logs mongodb

# Follow logs (stream realtime)
docker-compose logs -f airflow-webserver
```

---

## 🌐 Bước 4: Truy Cập Các Service

Sau khi tất cả containers đã up, bạn có thể truy cập:

| Service | URL | Username | Password |
|---------|-----|----------|----------|
| **Airflow** | `http://localhost:8080` | `airflow` | `airflow` |
| **PostgreSQL** | `localhost:5432` | `airflow_user` | `airflow_password` |
| **MongoDB** | `localhost:27017` | `admin` | `mongodb_password` |
| **Spark Master** | `http://localhost:8888` | - | - |

### 4.1 Kiểm tra Airflow Webserver

1. Mở browser
2. Truy cập: `http://localhost:8080`
3. Login với: `airflow / airflow`
4. Bạn sẽ thấy DAG `job_etl_dag` trong list

---

## 🧪 Bước 5: Chạy Pipeline

### 5.1 Cách 1: Trigger từ Airflow UI

1. Mở `http://localhost:8080`
2. Tìm DAG `job_etl_dag`
3. Nhấn vào DAG name
4. Nhấn nút **Trigger DAG** (mũi tên xanh)
5. Xem task execution graph

### 5.2 Cách 2: Chạy Script Thủ Công

#### Chạy Scraper:
```bash
# Chạy scraper script (lấy dữ liệu từ ITviec)
docker-compose exec airflow-webserver python /opt/airflow/scripts/ingestion/itviec_scraper.py
```

#### Chạy Spark Processing:
```bash
# Chạy spark processing script (làm sạch & trích xuất skill)
docker-compose exec airflow-webserver spark-submit /opt/airflow/scripts/processing/spark_cleaner.py
```

---

## 📊 Bước 6: Xem Kết Quả

### 6.1 Kiểm tra dữ liệu trong MongoDB

```bash
# Mở MongoDB shell
docker-compose exec mongodb mongosh -u admin -p mongodb_password

# Trong MongoDB shell:
use job_db
db.raw_jobs.find().limit(1).pretty()  # Xem 1 job
db.raw_jobs.count()                    # Đếm tổng jobs
```

### 6.2 Kiểm tra dữ liệu trong PostgreSQL

```bash
# Mở PostgreSQL client
docker-compose exec postgres psql -U airflow_user -d airflow_db

# Trong PostgreSQL:
SELECT COUNT(*) FROM fact_jobs;
SELECT * FROM fact_jobs LIMIT 5;
\d fact_jobs  -- Xem schema
```

### 6.3 Xem dữ liệu Parquet

```bash
# Dữ liệu Parquet được lưu tại:
# data/processed/jobs_processed/

# Có thể dùng Pandas để read
python
>>> import pandas as pd
>>> df = pd.read_parquet('data/processed/jobs_processed')
>>> df.head()
```

---

## 📈 Bước 7: Chạy Queries Phân Tích

### 7.1 Kết nối PostgreSQL và chạy queries

```bash
docker-compose exec postgres psql -U airflow_user -d airflow_db -f /opt/airflow/sql/queries.sql
```

### 7.2 Queries mẫu:

```sql
-- Top Skills được tìm kiếm
SELECT skill_name, COUNT(*) as count 
FROM dim_skills 
GROUP BY skill_name 
ORDER BY count DESC LIMIT 10;

-- Salary theo location
SELECT location, AVG(salary_max) as avg_salary 
FROM fact_jobs 
GROUP BY location 
ORDER BY avg_salary DESC;

-- Companies đang tuyển nhiều nhất
SELECT company_name, COUNT(*) as job_count 
FROM fact_jobs 
GROUP BY company_name 
ORDER BY job_count DESC;
```

---

## 🛑 Dừng & Cleanup

### Dừng tất cả containers:
```bash
docker-compose down
```

### Xóa tất cả dữ liệu (volumes):
```bash
docker-compose down -v
```

⚠️ **Cảnh báo**: Lệnh trên sẽ xóa tất cả dữ liệu trong MongoDB & PostgreSQL!

---

## 🐛 Troubleshooting

### Problem 1: "Port 5432 already in use"

**Giải pháp**: 
```bash
# Tìm process sử dụng port 5432
# Windows:
netstat -ano | findstr :5432

# Dừng container cũ
docker-compose down
docker ps -a  # xem tất cả containers
docker rm <container-id>
```

### Problem 2: "MongoDBConnectionError"

**Giải pháp**:
```bash
# Xem logs MongoDB
docker-compose logs mongodb

# Restart MongoDB
docker-compose restart mongodb

# Kiểm tra MongoDB status
docker-compose exec mongodb mongosh -u admin -p mongodb_password --eval "db.adminCommand('ping')"
```

### Problem 3: "Spark executor is not starting"

**Giải pháp**:
```bash
# RAM không đủ - giảm Spark executor memory
# Chỉnh lại docker-compose.yaml:
# SPARK_EXECUTOR_MEMORY=1g (thay vì 2g)

# Restart Spark
docker-compose restart spark-master spark-worker
```

### Problem 4: "DAG is not showing in Airflow UI"

**Giải pháp**:
```bash
# Kiểm tra DAG syntax
python -m py_compile airflow/dags/job_etl_dag.py

# Refresh Airflow UI (Ctrl+F5 hoặc clear cache)

# Check logs
docker-compose logs airflow-scheduler
```

---

## 📚 Tài Liệu Tham Khảo

- [Airflow Docs](https://airflow.apache.org/docs/)
- [PySpark Docs](https://spark.apache.org/docs/latest/api/python/)
- [MongoDB PyMongo](https://pymongo.readthedocs.io/)
- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [Docker Docs](https://docs.docker.com/)

---

## 📝 Notes

1. **Modify TARGET_URL**: Muốn cào từ khác URL? Sửa trong `.env`:
   ```
   TARGET_URL=https://itviec.com/it-jobs/junior-data-engineer
   ```

2. **Adjust Schedule**: Muốn chạy vào lúc khác? Sửa trong `job_etl_dag.py`:
   ```python
   schedule_interval='0 8 * * *'  # 8:00 AM mỗi ngày
   ```

3. **Increase Scrape Pages**: Muốn cào nhiều trang? Sửa trong `job_etl_dag.py`:
   ```python
   jobs = scraper.scrape_jobs(TARGET_URL, max_pages=5)  # Thay 5 từ 3
   ```

4. **Monitor Airflow Logs**:
   ```bash
   docker-compose logs -f airflow-scheduler
   ```

---

## ✅ Kiểm danh (Checklist)

- [ ] Docker Desktop đã cài & chạy
- [ ] Đã clone project vào thư mục
- [ ] Đã chạy `docker-compose build`
- [ ] Đã chạy `docker-compose up -d`
- [ ] Kiểm tra `docker-compose ps` → Tất cả containers Up
- [ ] Truy cập `http://localhost:8080` → Airflow UI
- [ ] Trigger `job_etl_dag` thành công
- [ ] Kiểm tra dữ liệu trong MongoDB
- [ ] Kiểm tra dữ liệu trong PostgreSQL
- [ ] Chạy sample queries từ `sql/queries.sql`

---

## 🎉 Hoàn Thành!

Nếu tất cả bước đã hoàn thành, **Chúc mừng!** 🎊

Bạn giờ đã có một **Data Engineering Pipeline hoàn chỉnh**:
- ✅ Tự động cào dữ liệu từ ITviec.com
- ✅ Xử lý dữ liệu với Spark
- ✅ Lưu trữ trong Data Warehouse (PostgreSQL)
- ✅ Điều phối với Airflow scheduler
- ✅ Phân tích & báo cáo dữ liệu

**Next Steps**:
1. Sửa chỉnh URL scraping, schedule, parameters theo nhu cầu
2. Thêm thêm logic xử lý (skill extraction, validation, etc)
3. Tạo thêm visualizations/dashboards (Tableau, Looker, etc)
4. Deploy lên cloud (AWS, GCP, Azure)

**Chúc bạn thành công!** 🚀

---

**Last Updated**: December 2025
