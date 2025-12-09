# 🏗️ Architecture & Technical Details

**Tài liệu chi tiết về kiến trúc, thiết kế, và quy trình của Data Engineering Pipeline**

---

## 📐 System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│                   INGESTION LAYER                           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         ITviec Scraper (Python + BS4)               │  │
│  │  - Gửi HTTP request đến ITviec.com                  │  │
│  │  - Parse HTML → Extract job info                    │  │
│  │  - Quản lý rate limiting & retry logic              │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │                                     │
│                       ▼                                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │        MongoDB (NoSQL - Document Store)             │  │
│  │  - Lưu raw JSON data từ scraper                     │  │
│  │  - Collection: raw_jobs                             │  │
│  │  - Unstructured data OK (khuyến khích!)             │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │                                     │
└───────────────────────┼─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│               PROCESSING LAYER (Spark)                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Spark Data Cleaner & Transformer            │  │
│  │  - Read từ MongoDB                                  │  │
│  │  - Clean: Remove HTML, emoji, normalize text        │  │
│  │  - Extract: Skill matching từ regex patterns        │  │
│  │  - Transform: Chuẩn hóa lương, location            │  │
│  │  - Explode: Job-Skill relationship                  │  │
│  │  - Save: Parquet + write to PostgreSQL              │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │                                     │
│                       ▼                                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │      Parquet Format (Columnar Storage)              │  │
│  │  - Location: data/processed/jobs_processed/         │  │
│  │  - Compressed & optimized for analytics             │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │                                     │
└───────────────────────┼─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│              SERVING LAYER (Data Warehouse)                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │      PostgreSQL (Star Schema)                       │  │
│  │                                                      │  │
│  │  FACT TABLE:         DIMENSION TABLES:             │  │
│  │  ┌────────────────┐  ┌──────────────────┐          │  │
│  │  │  fact_jobs    │  │  dim_companies   │          │  │
│  │  │  - job_id ✓   │  │  - company_id ✓ │          │  │
│  │  │  - company_id ├─→│  - company_name  │          │  │
│  │  │  - location   │  │  - industry      │          │  │
│  │  │  - salary_min │  └──────────────────┘          │  │
│  │  │  - salary_max │  ┌──────────────────┐          │  │
│  │  │  - posted_at  │  │  dim_skills      │          │  │
│  │  └────────┬───────┘  │  - skill_id ✓   │          │  │
│  │           │          │  - skill_name   │          │  │
│  │  ┌────────▼───────┐  └────────┬─────────┘          │  │
│  │  │bridge_job_skills│          │                   │  │
│  │  │- job_id ✓      ├──────────→│                   │  │
│  │  │- skill_id ✓────┤           │                   │  │
│  │  └────────────────┘           │                   │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│            ORCHESTRATION LAYER (Airflow)                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │           DAG: job_etl_dag                           │  │
│  │  - Schedule: Hàng ngày lúc 8:00 AM                  │  │
│  │  - Task 1: Scrape jobs                              │  │
│  │  - Task 2: Validate quality                         │  │
│  │  - Task 3: Setup DB schema                          │  │
│  │  - Task 4: Process data with Spark                  │  │
│  │  - Task 5: Load to warehouse                        │  │
│  │  - Task 6: Generate report                          │  │
│  │  - Task 7: Cleanup                                  │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│              ANALYTICS LAYER                                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Views (PostgreSQL):                                │  │
│  │  - v_top_skills: Top 20 skills by frequency        │  │
│  │  - v_salary_by_company: Benchmark theo công ty     │  │
│  │  - v_jobs_by_location: Job count theo vị trí       │  │
│  │  - v_skills_by_location: Skills phổ biến theo VT   │  │
│  │                                                      │  │
│  │  BI Tools (Future):                                 │  │
│  │  - Tableau / Looker / Power BI                      │  │
│  │  - Dashboards & Reports                            │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow (ETL Pipeline)

### Stage 1: Ingestion (Scraping)

```
ITviec.com
    ↓ (HTTP GET)
HTML Parser (BeautifulSoup)
    ↓ (Parse & Extract)
Raw Job Dictionary:
{
    'job_title': 'Data Engineer',
    'company_name': 'Google Vietnam',
    'location': 'Ho Chi Minh City',
    'salary': '$2000 - $4000',
    'description_preview': 'We are looking for...',
    'job_url': 'https://itviec.com/job/...',
    'source': 'itviec.com',
    'scraped_at': '2025-12-09T08:30:00'
}
    ↓ (Batch Insert)
MongoDB (Collection: raw_jobs)
```

### Stage 2: Processing (Transformation)

```
MongoDB raw_jobs
    ↓ (Read with Spark)
DataFrame {
    job_id, job_title, company_name, location, 
    salary_min, salary_max, description_preview
}
    ↓ (Clean Text)
Clean descriptions (remove HTML, emoji)
    ↓ (Normalize Salary)
Parse: "$2000 - $4000" → salary_min=2000, salary_max=4000
    ↓ (Extract Skills)
Regex matching: ['Python', 'Spark', 'AWS', 'SQL']
    ↓ (Explode)
One row per job-skill pair
    ↓ (Write Parquet)
Parquet files (columnar, compressed)
```

### Stage 3: Loading (to Data Warehouse)

```
Spark DataFrame
    ↓ (JDBC Driver)
PostgreSQL
    ├── fact_jobs (main table)
    ├── dim_companies (lookup)
    ├── dim_skills (lookup)
    └── bridge_job_skills (relationship)
```

---

## 🗄️ Database Schema (Star Schema)

### Fact Table: `fact_jobs`

| Column | Type | Description |
|--------|------|-------------|
| job_id | INT (PK) | Unique job identifier |
| company_id | INT (FK) | Reference to dim_companies |
| job_title | VARCHAR | Job position title |
| location | VARCHAR | Job location |
| salary_min | INT | Minimum salary (USD) |
| salary_max | INT | Maximum salary (USD) |
| posted_date | TIMESTAMP | When job was posted |
| scraped_at | TIMESTAMP | When data was scraped |
| processed_at | TIMESTAMP | When data was processed |
| data_quality_score | FLOAT | Quality metric (0-1) |

**Rationale**:
- Denormalized cho tốc độ query
- Salary tách min/max để dễ range query
- Timestamps cho data lineage tracking

### Dimension Tables

#### `dim_companies`
```
company_id (PK) → company_name → industry → created_at
```

#### `dim_skills`
```
skill_id (PK) → skill_name → skill_category → created_at
```

#### `bridge_job_skills` (Many-to-Many)
```
job_skill_id (PK) → job_id (FK) → skill_id (FK)
```

**Rationale**: 
- Mỗi job có nhiều skills
- Mỗi skill xuất hiện trong nhiều jobs
- Bridge table tối ưu query performance

---

## 🛠️ Technology Stack

### Ingestion
- **Python** 3.11+
- **requests**: HTTP client
- **BeautifulSoup4**: HTML parsing
- **PyMongo**: MongoDB driver

### Processing
- **PySpark** 3.5.0: Distributed computing
- **Pandas**: Data manipulation
- **NumPy**: Numerical operations

### Storage
- **MongoDB**: Raw data store (schemaless)
- **PostgreSQL**: Data Warehouse (schema-based)
- **Parquet**: Columnar format for analytics

### Orchestration
- **Apache Airflow** 2.7.3: Workflow orchestration
- **Scheduler**: Automated execution
- **UI**: Monitoring & debugging

### Infrastructure
- **Docker**: Containerization
- **Docker Compose**: Multi-container orchestration
- **Java 11**: Required for Spark & Airflow

---

## 🎯 Key Features

### 1. Skill Extraction Logic

```python
SKILLS_LIST = [
    'Python', 'Java', 'JavaScript', 'SQL', 'Spark', 
    'Docker', 'AWS', 'Kubernetes', ...
]

# Regex pattern: (Python|Java|JavaScript|...|Spark|...)
# Applied to lowercase description

# Example:
input: "We need a Data Engineer with Python, Spark, and AWS"
output: ['python', 'spark', 'aws']
```

**Approach**:
- Case-insensitive matching
- Simple pattern matching (not ML-based)
- Easy to maintain & update skills list
- ~80-90% accuracy for common skills

### 2. Salary Normalization

```
Input: "$2000 - $4000 per month"
Regex: \$(\d+)\s*-\s*\$(\d+)
Output: salary_min=2000, salary_max=4000
```

**Handles**:
- Multiple formats: "$2000 - $4000", "2000-4000", "up to $5000"
- Currency: USD (customizable)
- Edge cases: Single salary, "negotiable", missing data

### 3. Data Quality Scoring

```
score = (
    (has_salary * 0.2) +
    (has_location * 0.2) +
    (has_company * 0.2) +
    (description_length / max_length * 0.2) +
    (skills_found / expected_skills * 0.2)
)
```

---

## 📊 Data Statistics Example

**After 1 week of scraping**:

| Metric | Value |
|--------|-------|
| Total Jobs Scraped | 2,500 |
| Unique Companies | 350 |
| Unique Locations | 15 |
| Unique Skills | 200 |
| Avg Jobs per Company | 7.1 |
| Avg Salary | $2,500/month |

---

## ⚙️ Configuration

### Environment Variables (.env)

| Variable | Default | Description |
|----------|---------|-------------|
| MONGO_HOST | localhost | MongoDB host |
| MONGO_PORT | 27017 | MongoDB port |
| POSTGRES_HOST | localhost | PostgreSQL host |
| POSTGRES_PORT | 5432 | PostgreSQL port |
| TARGET_URL | https://... | URL to scrape |
| SCRAPE_DELAY | 2 | Delay between requests (seconds) |

### Spark Configuration

```python
spark = SparkSession.builder \
    .appName("ITviec-ETL") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.cores.max", "4") \
    .getOrCreate()
```

### Airflow Configuration

```python
schedule_interval = '0 8 * * *'  # 8:00 AM daily
retries = 2
retry_delay = timedelta(minutes=5)
catchup = False
```

---

## 🔐 Security Considerations

### 1. Credentials Management
- ✅ All credentials in `.env` (git-ignored)
- ❌ NEVER commit `.env` to git
- ✅ Use environment variables in production

### 2. Database Access
- PostgreSQL: Only accessible from Airflow/Spark containers
- MongoDB: No external access in docker-compose
- Credentials: Stored in environment variables

### 3. Web Scraping Ethics
- ✅ Respect `robots.txt`
- ✅ Rate limiting (SCRAPE_DELAY=2 seconds)
- ✅ User-Agent header
- ❌ Caching credentials in code

---

## 📈 Scaling Considerations

### Current Setup (Single Node)
- Max ~100k jobs per run
- ~5-10 minutes processing
- Suitable for learning & development

### Scaling to Production

#### 1. Increase Volume
```python
# config.yaml
max_pages: 10  # Instead of 3
batch_size: 5000  # Process in chunks
```

#### 2. Distributed Spark
```yaml
# docker-compose.yaml
spark:
  cluster_mode: "distributed"
  num_workers: 4  # Instead of 1
  executor_memory: "4g"  # Instead of 2g
```

#### 3. Database Optimization
```sql
-- Add partitioning
CREATE TABLE fact_jobs_2025 PARTITION BY RANGE (posted_date)
AS SELECT * FROM fact_jobs;

-- Materialized views
CREATE MATERIALIZED VIEW mv_top_skills AS ...;
```

#### 4. Add Data Lake
```
s3://company-data-lake/
├── raw/
│   └── itviec/2025-12-09/...
├── processed/
│   └── itviec/2025-12-09/...
└── warehouse/
    └── itviec/fact_jobs/
```

---

## 🧪 Testing & Validation

### Unit Tests

```python
# test_scraper.py
def test_job_parsing():
    html = '<div class="job-item">...</div>'
    job = scraper.parse_job_listing(html)
    assert job['job_title'] == 'Data Engineer'

def test_salary_normalization():
    salary = "$2000 - $4000"
    min_sal, max_sal = normalize_salary(salary)
    assert min_sal == 2000 and max_sal == 4000
```

### Data Quality Checks

```python
# In DAG
def validate_data_quality(**context):
    # Check row count
    assert count > min_threshold
    
    # Check nulls
    assert df.filter(col('job_title').isNull()).count() == 0
    
    # Check salary range
    assert (df.salary_max >= df.salary_min).all()
```

### Integration Tests

```bash
# Run full pipeline
docker-compose exec airflow-webserver \
  python -m pytest tests/integration/
```

---

## 📝 Common Customizations

### Change Scraping Target

```python
# Option 1: Change in .env
TARGET_URL=https://topdev.vn/jobs?page=1

# Option 2: Create new scraper
class TopdevScraper(ITviecScraper):
    def parse_job_listing(self, job_html):
        # Custom parsing logic
        pass
```

### Add New Skills to Extract

```python
# In spark_cleaner.py
SKILLS_LIST.extend([
    'Kotlin', 'Go', 'Rust', 'Elixir',
    'WebAssembly', 'CUDA'
])
```

### Change ETL Schedule

```python
# In job_etl_dag.py
schedule_interval = '0 */6 * * *'  # Every 6 hours
# Or
schedule_interval = '0 0 * * 0'    # Every Sunday
```

---

## 🚀 Performance Optimization Tips

1. **Reduce Scraping Pages**: `max_pages=1` instead of 3 (faster testing)
2. **Increase Spark Parallelism**: `.config("spark.sql.shuffle.partitions", "200")`
3. **Cache DataFrames**: `df.cache()` before multiple operations
4. **Use Indexes**: Create indexes on `job_id`, `company_id`, `skill_id`
5. **Compress Data**: Parquet with snappy compression

---

## 📚 References

- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [PySpark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [MongoDB Schema Design](https://docs.mongodb.com/manual/core/data-modeling-introduction/)
- [PostgreSQL Star Schema](https://wiki.postgresql.org/wiki/Performance_Optimization)

---

**Last Updated**: December 2025
**Version**: 1.0
