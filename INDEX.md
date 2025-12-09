# 📚 Documentation Index - VN IT Job Analytics

**Hướng dẫn nhanh để tìm tài liệu cần thiết**

---

## 🎯 Nếu bạn muốn... (Chọn theo nhu cầu của bạn)

### ✅ BẮT ĐẦU NGAY (Start Here!)
1. **[README.md](README.md)** - Tổng quan 5 phút
2. **[SETUP_GUIDE.md](SETUP_GUIDE.md)** - Hướng dẫn setup chi tiết (⭐ READ THIS FIRST!)
3. **[PROJECT_SUMMARY.txt](PROJECT_SUMMARY.txt)** - Tóm tắt project

### 📖 HIỂU RÕ KIẾN TRÚC
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Deep dive technical details
- **[README.md](README.md#-kiến-trúc-luồng-dữ-liệu-pipeline-architecture)** - Kiến trúc pipeline

### 🐳 SETUP & CHẠY PROJECT
1. **[SETUP_GUIDE.md](SETUP_GUIDE.md#-bước-1-chuẩn-bị-môi-trường)** - Setup từng bước
2. **[CHEATSHEET.md](CHEATSHEET.md#-docker--docker-compose)** - Docker commands
3. **[SETUP_GUIDE.md](SETUP_GUIDE.md#-troubleshooting)** - Troubleshooting

### 🔧 VIẾT CODE
- **[scripts/ingestion/itviec_scraper.py](scripts/ingestion/itviec_scraper.py)** - Scraper code
- **[scripts/processing/spark_cleaner.py](scripts/processing/spark_cleaner.py)** - Spark code
- **[airflow/dags/job_etl_dag.py](airflow/dags/job_etl_dag.py)** - Airflow DAG

### 📊 QUERIES & ANALYSIS
- **[sql/init_db.sql](sql/init_db.sql)** - Database schema
- **[sql/queries.sql](sql/queries.sql)** - 14 sample queries

### ⚡ CHEAT SHEET & QUICK COMMANDS
- **[CHEATSHEET.md](CHEATSHEET.md)** - Docker, Airflow, SQL commands

### 🐛 CÓ VẤN ĐỀ?
- **[SETUP_GUIDE.md](SETUP_GUIDE.md#-bước-6-troubleshooting)** - Troubleshooting guide
- **[CHEATSHEET.md](CHEATSHEET.md#-emergency-commands)** - Emergency commands

---

## 📂 File Structure & Purpose

```
vn-it-job-analytics/
│
├── 📌 DOCUMENTATION (Bạn đang đọc ở đây)
│   ├── INDEX.md                 ← You are here
│   ├── README.md               ✅ Read this first (5 min)
│   ├── SETUP_GUIDE.md          ✅ Setup instructions (IMPORTANT!)
│   ├── ARCHITECTURE.md         📖 Technical details
│   ├── CHEATSHEET.md           ⚡ Quick commands
│   ├── PROJECT_SUMMARY.txt     📊 Project stats
│
├── 🐳 DOCKER CONFIGURATION
│   ├── docker-compose.yaml     - Orchestrates all services
│   ├── docker/
│   │   ├── airflow.Dockerfile  - Airflow image
│   │   └── spark.Dockerfile    - Spark image
│
├── ⚙️ CONFIGURATION
│   ├── .env                    - Environment variables
│   ├── requirements.txt        - Python dependencies
│   ├── .gitignore              - Git config
│
├── 🔄 ORCHESTRATION (Airflow)
│   └── airflow/
│       ├── dags/
│       │   └── job_etl_dag.py  - Main ETL workflow
│       ├── logs/               - Generated logs
│       └── plugins/            - Custom plugins
│
├── 🐍 SCRIPTS (Code)
│   ├── scripts/
│   │   ├── ingestion/
│   │   │   ├── __init__.py
│   │   │   └── itviec_scraper.py    - Web scraper
│   │   └── processing/
│   │       ├── __init__.py
│   │       └── spark_cleaner.py     - Data processing
│
├── 🗄️ DATABASE (SQL)
│   └── sql/
│       ├── init_db.sql        - Schema definition
│       └── queries.sql        - Sample queries (14 queries)
│
└── 📊 DATA (Generated)
    └── data/
        ├── raw/              - JSON from scraper
        └── processed/        - Parquet from Spark
```

---

## 🎬 Quick Navigation

### For Different Roles:

#### 👨‍💻 **Developer** (Muốn sửa code)
1. [SETUP_GUIDE.md](SETUP_GUIDE.md) - Setup local environment
2. [scripts/ingestion/itviec_scraper.py](scripts/ingestion/itviec_scraper.py) - Scraper logic
3. [scripts/processing/spark_cleaner.py](scripts/processing/spark_cleaner.py) - Processing logic
4. [airflow/dags/job_etl_dag.py](airflow/dags/job_etl_dag.py) - DAG orchestration
5. [CHEATSHEET.md](CHEATSHEET.md) - Common commands

#### 📊 **Data Analyst** (Muốn query dữ liệu)
1. [SETUP_GUIDE.md](SETUP_GUIDE.md) - Setup infrastructure
2. [sql/init_db.sql](sql/init_db.sql) - Understand schema
3. [sql/queries.sql](sql/queries.sql) - Read sample queries
4. [CHEATSHEET.md](CHEATSHEET.md#-postgresql-commands) - PostgreSQL commands

#### 🏗️ **DevOps/Infrastructure** (Muốn quản lý infra)
1. [docker-compose.yaml](docker-compose.yaml) - Infra definition
2. [docker/airflow.Dockerfile](docker/airflow.Dockerfile) - Airflow container
3. [docker/spark.Dockerfile](docker/spark.Dockerfile) - Spark container
4. [CHEATSHEET.md](CHEATSHEET.md#-docker--docker-compose) - Docker commands
5. [SETUP_GUIDE.md](SETUP_GUIDE.md) - Deployment guide

#### 📚 **Student/Learner** (Muốn học)
1. [README.md](README.md) - Overview
2. [ARCHITECTURE.md](ARCHITECTURE.md) - Learn architecture
3. [SETUP_GUIDE.md](SETUP_GUIDE.md) - Follow tutorial
4. All source code files - Read comments
5. [sql/queries.sql](sql/queries.sql) - Learn SQL patterns

---

## 📝 Documentation Map

```
Quick Start?
  ↓
  README.md (5 min)
    ↓
    Want to run it?
      ↓
      SETUP_GUIDE.md ← Start here! (Follow step by step)
        ↓
        Need commands?
          ↓
          CHEATSHEET.md
        
        Having issues?
          ↓
          SETUP_GUIDE.md → Troubleshooting

Want to understand architecture?
  ↓
  ARCHITECTURE.md (deep dive)
    ↓
    Understand schema?
      ↓
      sql/init_db.sql
        ↓
        Want to analyze?
          ↓
          sql/queries.sql

Want to modify code?
  ↓
  scripts/ingestion/itviec_scraper.py
  scripts/processing/spark_cleaner.py
  airflow/dags/job_etl_dag.py
    ↓
    CHEATSHEET.md (for commands)

Need stats?
  ↓
  PROJECT_SUMMARY.txt
```

---

## 🔍 Search Guide

### Nếu bạn cần tìm...

| Nếu muốn... | Tìm trong... | Dòng |
|---|---|---|
| Setup Docker | [SETUP_GUIDE.md](SETUP_GUIDE.md#-bước-3-khởi-động-infrastructure) | Step 3 |
| Run pipeline | [SETUP_GUIDE.md](SETUP_GUIDE.md#-bước-5-chạy-pipeline) | Step 5 |
| Troubleshoot | [SETUP_GUIDE.md](SETUP_GUIDE.md#-troubleshooting) | Section 7 |
| Docker commands | [CHEATSHEET.md](CHEATSHEET.md#-docker--docker-compose) | Top section |
| SQL queries | [sql/queries.sql](sql/queries.sql) | Numbered 1-14 |
| Scraper code | [scripts/ingestion/itviec_scraper.py](scripts/ingestion/itviec_scraper.py) | Class ITviecScraper |
| Spark processing | [scripts/processing/spark_cleaner.py](scripts/processing/spark_cleaner.py) | Class SparkDataCleaner |
| Airflow DAG | [airflow/dags/job_etl_dag.py](airflow/dags/job_etl_dag.py) | 7 Tasks |
| Database schema | [sql/init_db.sql](sql/init_db.sql) | Tables section |
| Environment vars | [.env](.env) | All lines |
| Architecture | [ARCHITECTURE.md](ARCHITECTURE.md) | Section 1-2 |
| Project stats | [PROJECT_SUMMARY.txt](PROJECT_SUMMARY.txt) | Mid section |

---

## ⏱️ Time Estimates

| Task | Time | Documentation |
|------|------|-----------------|
| Read README | 5 min | [README.md](README.md) |
| Complete setup | 15-30 min | [SETUP_GUIDE.md](SETUP_GUIDE.md) |
| Run first pipeline | 10-15 min | [SETUP_GUIDE.md](SETUP_GUIDE.md#-bước-5-chạy-pipeline) |
| Understand architecture | 20-30 min | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Learn entire system | 2-3 hours | All docs + code |
| Modify & customize | 1-2 hours | Code files + [CHEATSHEET.md](CHEATSHEET.md) |

---

## 📞 Getting Help

### Bước 1: Kiểm tra Quick Answers
```
Problem with Docker?
  → CHEATSHEET.md → DOCKER section
  
Problem with Airflow?
  → CHEATSHEET.md → AIRFLOW COMMANDS
  
Problem with PostgreSQL?
  → CHEATSHEET.md → POSTGRESQL COMMANDS
  
Having setup issues?
  → SETUP_GUIDE.md → TROUBLESHOOTING
```

### Bước 2: Debug
```
1. Check logs:
   docker-compose logs <service-name>

2. Verify containers:
   docker-compose ps

3. Check if port is open:
   netstat -ano | findstr :5432 (PostgreSQL)
   netstat -ano | findstr :8080 (Airflow)
```

### Bước 3: Reset & Retry
```
# Last resort - clean everything
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

---

## 🎓 Learning Path

**Recommended order to learn the entire project:**

1. ✅ **[README.md](README.md)** (5 min)
   - Overview & features

2. ✅ **[SETUP_GUIDE.md](SETUP_GUIDE.md)** (30 min)
   - Follow all steps
   - Get everything running

3. ✅ **[PROJECT_SUMMARY.txt](PROJECT_SUMMARY.txt)** (10 min)
   - Project stats & highlights

4. ✅ **[ARCHITECTURE.md](ARCHITECTURE.md)** (30 min)
   - Read slowly, understand flow
   - Look at diagrams

5. ✅ **[scripts/ingestion/itviec_scraper.py](scripts/ingestion/itviec_scraper.py)** (20 min)
   - Read class & methods
   - Understand scraping logic

6. ✅ **[scripts/processing/spark_cleaner.py](scripts/processing/spark_cleaner.py)** (20 min)
   - Read class & methods
   - Understand data transformation

7. ✅ **[airflow/dags/job_etl_dag.py](airflow/dags/job_etl_dag.py)** (15 min)
   - Understand DAG structure
   - Task dependencies

8. ✅ **[sql/init_db.sql](sql/init_db.sql)** (15 min)
   - Schema design
   - Tables & relationships

9. ✅ **[sql/queries.sql](sql/queries.sql)** (15 min)
   - Sample queries
   - Data analysis examples

10. ✅ **[CHEATSHEET.md](CHEATSHEET.md)** (Reference)
    - Keep for daily use

**Total time: ~3 hours** → Deep understanding! 🎉

---

## 💾 File Download / Copy

```bash
# Clone entire project
git clone <repo-url>
cd vn-it-job-analytics

# Copy single file from project
cp sql/queries.sql my-queries.sql

# View file without cloning
# → Open in browser or text editor
```

---

## 📌 Bookmarks (For Quick Access)

Save these URLs in your browser:

- **Local Airflow UI**: `http://localhost:8080`
  - Login: airflow / airflow
  - Access after `docker-compose up -d`

- **Local MongoDB**: `mongodb://admin:mongodb_password@localhost:27017/job_db`

- **Local PostgreSQL**: `postgresql://airflow_user:airflow_password@localhost:5432/airflow_db`

---

## 🎯 Next Steps After Reading

1. **✅ Setup** → Follow [SETUP_GUIDE.md](SETUP_GUIDE.md) completely
2. **✅ Run** → Trigger first pipeline
3. **✅ Analyze** → Run queries from [sql/queries.sql](sql/queries.sql)
4. **✅ Customize** → Modify code in [scripts/](scripts/)
5. **✅ Deploy** → Use [docker-compose.yaml](docker-compose.yaml) in production

---

## 📞 Support Resources

- **Docker Docs**: https://docs.docker.com/
- **Airflow Docs**: https://airflow.apache.org/docs/
- **Spark Docs**: https://spark.apache.org/docs/
- **PostgreSQL Docs**: https://www.postgresql.org/docs/
- **MongoDB Docs**: https://docs.mongodb.com/

---

## 🎉 You're All Set!

**Recommended reading order:**
```
README.md (5 min)
    ↓
SETUP_GUIDE.md (30 min) ← DO THIS FIRST!
    ↓
PROJECT_SUMMARY.txt (10 min)
    ↓
ARCHITECTURE.md (30 min)
    ↓
Code files + CHEATSHEET.md
```

**Happy learning & coding!** 🚀

---

**Last Updated**: December 2025
**Version**: 1.0
**Total Pages**: 13 docs + 7 code files
**Total Words**: ~25,000 lines
