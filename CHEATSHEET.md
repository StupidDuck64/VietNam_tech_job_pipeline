# 🚀 CHEATSHEET - Các Lệnh Thường Dùng

**Quick reference cho các lệnh Docker, Airflow, Spark, SQL**

---

## 🐳 DOCKER & DOCKER-COMPOSE

```bash
# ===== Khởi động =====
docker-compose up -d                # Khởi động tất cả services
docker-compose up -d postgres        # Khởi động 1 service
docker-compose build                 # Build images
docker-compose build --no-cache      # Build lại từ đầu

# ===== Kiểm tra Status =====
docker-compose ps                    # Liệt kê containers
docker-compose logs airflow-scheduler # Xem logs scheduler
docker-compose logs -f airflow-webserver # Follow logs realtime
docker-compose logs postgres          # Xem logs PostgreSQL

# ===== Dừng & Cleanup =====
docker-compose down                  # Dừng tất cả
docker-compose down -v               # Dừng + xóa volumes (DATA LOSS!)
docker-compose restart airflow-scheduler # Restart 1 service
docker-compose pause postgres        # Pause (không xóa)
docker-compose unpause postgres      # Resume

# ===== Debug =====
docker-compose exec airflow-webserver bash  # SSH vào container
docker-compose exec postgres psql -U airflow_user -d airflow_db  # Vào PostgreSQL
docker-compose exec mongodb mongosh -u admin -p mongodb_password  # Vào MongoDB
```

---

## 🔄 AIRFLOW COMMANDS

```bash
# ===== Trigger DAG =====
docker-compose exec airflow-webserver airflow dags trigger job_etl_dag
docker-compose exec airflow-webserver airflow dags trigger job_etl_dag --conf '{"key":"value"}'

# ===== Kiểm tra DAG =====
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow dags test job_etl_dag 2025-12-09

# ===== Task operations =====
docker-compose exec airflow-webserver airflow tasks list job_etl_dag
docker-compose exec airflow-webserver airflow tasks test job_etl_dag scrape_jobs 2025-12-09
docker-compose exec airflow-webserver airflow tasks clear job_etl_dag  # Clear run history

# ===== User management =====
docker-compose exec airflow-webserver airflow users list
docker-compose exec airflow-webserver airflow users create \
  --role Admin \
  --username admin \
  --email admin@example.com \
  --firstname Admin \
  --lastname User \
  --password admin123

# ===== Database operations =====
docker-compose exec airflow-webserver airflow db migrate   # Run migrations
docker-compose exec airflow-webserver airflow db upgrade   # Upgrade DB
docker-compose exec airflow-webserver airflow db reset     # Reset (CAREFUL!)

# ===== View Airflow config =====
docker-compose exec airflow-webserver airflow config list  # Show config
docker-compose exec airflow-webserver airflow info         # System info
```

---

## 🐍 PYTHON & SCRIPTS

```bash
# ===== Run Python scripts =====
docker-compose exec airflow-webserver python /opt/airflow/scripts/ingestion/itviec_scraper.py
docker-compose exec airflow-webserver python -m scripts.ingestion.itviec_scraper

# ===== Python interactive shell =====
docker-compose exec airflow-webserver python
# >>> import requests
# >>> import pymongo
# >>> exit()

# ===== Check syntax =====
docker-compose exec airflow-webserver python -m py_compile airflow/dags/job_etl_dag.py
docker-compose exec airflow-webserver python -m py_compile scripts/ingestion/itviec_scraper.py

# ===== Install package =====
docker-compose exec airflow-webserver pip install pandas==2.1.1
docker-compose exec airflow-webserver pip list
```

---

## ⚡ SPARK COMMANDS

```bash
# ===== Submit Spark job =====
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/processing/spark_cleaner.py

# ===== Spark interactive shell =====
docker-compose exec spark-master spark-shell
docker-compose exec spark-master pyspark

# ===== View Spark logs =====
docker-compose logs spark-master
docker-compose logs spark-worker

# ===== Access Spark UI =====
# Browser: http://localhost:8888
```

---

## 🗄️ POSTGRESQL COMMANDS

```bash
# ===== Connect =====
docker-compose exec postgres psql -U airflow_user -d airflow_db

# ===== Inside PostgreSQL =====
\d                           # List tables
\d fact_jobs                 # Describe table
\dt                          # List all tables
\dv                          # List views
SELECT * FROM fact_jobs LIMIT 5;  # Query
SELECT COUNT(*) FROM fact_jobs;   # Count rows
\copy fact_jobs TO '/tmp/export.csv' CSV  # Export

# ===== Create backup =====
docker-compose exec postgres pg_dump -U airflow_user airflow_db > backup.sql

# ===== Restore from backup =====
docker-compose exec postgres psql -U airflow_user airflow_db < backup.sql

# ===== Exit =====
\q

# ===== Run SQL file =====
docker-compose exec postgres psql -U airflow_user -d airflow_db -f /opt/airflow/sql/init_db.sql
```

---

## 🍃 MONGODB COMMANDS

```bash
# ===== Connect =====
docker-compose exec mongodb mongosh -u admin -p mongodb_password

# ===== Inside MongoDB =====
show dbs                     # List databases
use job_db                   # Select database
show collections             # List collections
db.raw_jobs.find().limit(5)  # Get 5 documents
db.raw_jobs.count()          # Count documents
db.raw_jobs.findOne()        # Get 1 document
db.raw_jobs.find({company_name: "Google"})  # Filter

# ===== Delete all =====
db.raw_jobs.deleteMany({})   # Clear collection

# ===== Export/Import =====
mongoexport --uri="mongodb://admin:mongodb_password@localhost:27017/job_db" \
  --collection=raw_jobs --out=jobs.json
mongoimport --uri="mongodb://admin:mongodb_password@localhost:27017/job_db" \
  --collection=raw_jobs --file=jobs.json

# ===== Exit =====
exit
```

---

## 📊 SQL QUERIES (Common)

```sql
-- ===== Count records =====
SELECT COUNT(*) FROM fact_jobs;

-- ===== Top 10 Skills =====
SELECT skill_name, COUNT(*) as count FROM dim_skills 
GROUP BY skill_name ORDER BY count DESC LIMIT 10;

-- ===== Salary by Location =====
SELECT location, AVG(salary_max) as avg_salary FROM fact_jobs
WHERE location IS NOT NULL AND salary_max IS NOT NULL
GROUP BY location ORDER BY avg_salary DESC;

-- ===== Top Companies =====
SELECT company_name, COUNT(*) as job_count FROM fact_jobs fj
JOIN dim_companies dc ON fj.company_id = dc.company_id
GROUP BY company_name ORDER BY job_count DESC LIMIT 20;

-- ===== Jobs with Skills =====
SELECT fj.job_title, STRING_AGG(ds.skill_name, ', ') as skills
FROM fact_jobs fj
LEFT JOIN bridge_job_skills bjs ON fj.job_id = bjs.job_id
LEFT JOIN dim_skills ds ON bjs.skill_id = ds.skill_id
GROUP BY fj.job_id, fj.job_title LIMIT 10;

-- ===== Data Quality =====
SELECT 
  COUNT(*) as total_jobs,
  COUNT(CASE WHEN salary_min IS NOT NULL THEN 1 END) as with_salary,
  COUNT(CASE WHEN location IS NOT NULL THEN 1 END) as with_location
FROM fact_jobs;

-- ===== Clear all data =====
DELETE FROM fact_jobs;
DELETE FROM dim_companies;
DELETE FROM dim_skills;
```

---

## 🔍 LOGS & MONITORING

```bash
# ===== View specific logs =====
docker-compose logs airflow-scheduler --tail=50  # Last 50 lines
docker-compose logs airflow-webserver --tail=100 # Last 100 lines
docker-compose logs --since 5m                    # Last 5 minutes
docker-compose logs --until 10m                   # Until 10min ago

# ===== Real-time monitoring =====
docker-compose logs -f                           # Follow all
docker-compose logs -f airflow-scheduler postgres  # Follow multiple

# ===== Save logs =====
docker-compose logs > logs.txt                   # Save to file
docker-compose logs airflow-webserver > airflow.log

# ===== Docker system info =====
docker ps -a                          # All containers (including stopped)
docker images                          # All images
docker volume ls                       # All volumes
docker network ls                      # All networks
docker stats                           # CPU/Memory usage

# ===== Cleanup (Danger!) =====
docker system prune                   # Remove unused images/containers
docker volume prune                   # Remove unused volumes
```

---

## 📝 FILE OPERATIONS

```bash
# ===== Inside container =====
docker-compose exec airflow-webserver ls -la /opt/airflow/
docker-compose exec airflow-webserver cat /opt/airflow/.env
docker-compose exec airflow-webserver cp source.txt dest.txt

# ===== Copy to/from container =====
docker-compose cp postgres:/var/lib/postgresql/data backup/  # Copy FROM
docker-compose cp local.sql airflow-webserver:/tmp/          # Copy TO

# ===== View file in container =====
docker-compose exec airflow-webserver less /opt/airflow/logs/error.log
docker-compose exec airflow-webserver grep "ERROR" /opt/airflow/logs/*.log
docker-compose exec airflow-webserver tail -f /opt/airflow/logs/scheduler.log
```

---

## 🎯 COMMON WORKFLOWS

### Workflow 1: Full Pipeline Run

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Wait for services to be healthy
sleep 30
docker-compose ps

# 3. Trigger DAG
docker-compose exec airflow-webserver airflow dags trigger job_etl_dag

# 4. Monitor execution
docker-compose logs -f airflow-scheduler

# 5. Check results in PostgreSQL
docker-compose exec postgres psql -U airflow_user -d airflow_db -c \
  "SELECT COUNT(*) FROM fact_jobs;"

# 6. Check MongoDB
docker-compose exec mongodb mongosh -u admin -p mongodb_password \
  --eval "db.job_db.raw_jobs.countDocuments()"
```

### Workflow 2: Debug a Task

```bash
# 1. Get into container
docker-compose exec airflow-webserver bash

# 2. Check DAG syntax
python -m py_compile /opt/airflow/dags/job_etl_dag.py

# 3. Test specific task
airflow tasks test job_etl_dag scrape_jobs 2025-12-09

# 4. View task logs
tail -f /opt/airflow/logs/job_etl_dag/scrape_jobs/*.log

# 5. Exit
exit
```

### Workflow 3: Reset Everything

```bash
# 1. Stop services
docker-compose down

# 2. Remove volumes (DELETE DATA!)
docker-compose down -v

# 3. Clear docker
docker system prune -f

# 4. Rebuild
docker-compose build --no-cache

# 5. Start fresh
docker-compose up -d
```

### Workflow 4: Run Script Manually

```bash
# 1. Enter Airflow container
docker-compose exec airflow-webserver bash

# 2. Run scraper
python /opt/airflow/scripts/ingestion/itviec_scraper.py

# 3. View MongoDB results
mongosh -u admin -p mongodb_password job_db

# 4. Run Spark processor
spark-submit /opt/airflow/scripts/processing/spark_cleaner.py

# 5. Check Parquet output
python
>>> import pandas as pd
>>> df = pd.read_parquet('/opt/airflow/data/processed/jobs_processed')
>>> df.shape
>>> df.head()
>>> exit()

# 6. Exit
exit
```

---

## 🆘 EMERGENCY COMMANDS

```bash
# ===== Stop everything =====
docker-compose down

# ===== Kill all containers =====
docker stop $(docker ps -q)
docker rm $(docker ps -aq)

# ===== Reset database (DATA LOSS!) =====
docker-compose exec postgres psql -U airflow_user -d airflow_db -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
docker-compose exec mongodb mongosh -u admin -p mongodb_password --eval "db.raw_jobs.deleteMany({})"

# ===== Free up space =====
docker system prune -a --volumes
docker builder prune

# ===== Check disk usage =====
docker system df
```

---

## 💡 TIPS & TRICKS

```bash
# ===== Quick Airflow status =====
alias af-status='docker-compose ps'
alias af-logs='docker-compose logs -f airflow-scheduler'

# ===== Quick database access =====
alias psql-connect='docker-compose exec postgres psql -U airflow_user -d airflow_db'
alias mongo-connect='docker-compose exec mongodb mongosh -u admin -p mongodb_password'

# ===== Monitor in one terminal =====
watch 'docker-compose ps && echo "---" && docker stats --no-stream'

# ===== Tail multiple logs =====
docker-compose logs -f airflow-scheduler airflow-webserver postgres 2>&1 | grep ERROR

# ===== Count lines in log =====
docker-compose logs airflow-scheduler | wc -l
```

---

## 📚 File Paths Reference

```
📁 Project Root
├── 📁 airflow/
│   ├── 📁 dags/
│   │   └── 📄 job_etl_dag.py      ← Main DAG
│   ├── 📁 logs/                    ← Airflow logs
│   └── 📁 plugins/
├── 📁 scripts/
│   ├── 📁 ingestion/
│   │   └── 📄 itviec_scraper.py    ← Scraper script
│   └── 📁 processing/
│       └── 📄 spark_cleaner.py     ← Spark script
├── 📁 data/
│   ├── 📁 raw/                     ← MongoDB data (JSON)
│   └── 📁 processed/               ← Spark output (Parquet)
├── 📁 sql/
│   ├── 📄 init_db.sql              ← Schema
│   └── 📄 queries.sql              ← Sample queries
└── 📁 docker/
    ├── 📄 airflow.Dockerfile
    └── 📄 spark.Dockerfile

Container Mount Paths:
/opt/airflow/                       ← Airflow home
/opt/airflow/dags/                  ← DAGs folder
/opt/airflow/logs/                  ← Logs folder
/opt/airflow/scripts/               ← Python scripts
/opt/airflow/sql/                   ← SQL files
/opt/airflow/data/                  ← Data folder
```

---

## 📊 Port Reference

| Service | Port | URL |
|---------|------|-----|
| Airflow | 8080 | http://localhost:8080 |
| Spark UI | 8888 | http://localhost:8888 |
| PostgreSQL | 5432 | localhost:5432 |
| MongoDB | 27017 | localhost:27017 |
| Airflow Webserver | 8080 | http://localhost:8080 |

---

## ⏱️ Common Timing

```bash
docker-compose build              # 3-5 minutes
docker-compose up -d              # 1-2 minutes
Full ETL pipeline                 # 5-10 minutes
Airflow DAG execution             # 5-15 minutes
Scraper (3 pages)                 # 2-3 minutes
Spark processing                  # 2-5 minutes
```

---

## 🔐 Default Credentials

```
Airflow:
  Username: airflow
  Password: airflow

PostgreSQL:
  User: airflow_user
  Password: airflow_password
  Database: airflow_db

MongoDB:
  Username: admin
  Password: mongodb_password
  Database: job_db
```

---

**Last Updated**: December 2025
**Print & Keep Handy!** 📌
