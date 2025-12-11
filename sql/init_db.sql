-- ===== 1. DIMENSION TABLE - Công ty =====
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL UNIQUE,
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===== 2. FACT TABLE - Bảng chứa dữ liệu chính về jobs =====
CREATE TABLE IF NOT EXISTS fact_jobs (
    job_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    job_url VARCHAR(500),
    location VARCHAR(100),
    salary_min INT,
    salary_max INT,
    posted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scraped_at TIMESTAMP,
    processed_at TIMESTAMP,
    data_quality_score FLOAT DEFAULT 1.0,
    FOREIGN KEY (company_id) REFERENCES dim_companies(company_id)
);

-- ===== 3. DIMENSION TABLE - Kỹ năng =====
CREATE TABLE IF NOT EXISTS dim_skills (
    skill_id SERIAL PRIMARY KEY,
    skill_name VARCHAR(100) NOT NULL UNIQUE,
    skill_category VARCHAR(50),  -- e.g., 'Backend', 'Frontend', 'Data', 'DevOps'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===== 4. BRIDGE TABLE - Mối quan hệ Job-Skill (Many-to-Many) =====
CREATE TABLE IF NOT EXISTS bridge_job_skills (
    job_skill_id SERIAL PRIMARY KEY,
    job_id INT NOT NULL,
    skill_id INT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES fact_jobs(job_id) ON DELETE CASCADE,
    FOREIGN KEY (skill_id) REFERENCES dim_skills(skill_id),
    UNIQUE(job_id, skill_id)
);

-- ===== 5. DIMENSION TABLE - Vị trí =====
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL UNIQUE,
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===== 6. STAGING TABLE - Dữ liệu raw chưa xử lý =====
CREATE TABLE IF NOT EXISTS staging_raw_jobs (
    staging_id SERIAL PRIMARY KEY,
    job_title VARCHAR(255),
    company_name VARCHAR(255),
    location VARCHAR(100),
    salary VARCHAR(100),
    description_preview TEXT,
    job_url VARCHAR(500),
    source VARCHAR(50),
    scraped_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===== CREATE INDEXES - Tối ưu query performance =====
-- Sử dụng IF NOT EXISTS để tránh lỗi khi chạy lại
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fact_jobs_company_id') THEN
        CREATE INDEX idx_fact_jobs_company_id ON fact_jobs(company_id);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fact_jobs_location') THEN
        CREATE INDEX idx_fact_jobs_location ON fact_jobs(location);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fact_jobs_posted_date') THEN
        CREATE INDEX idx_fact_jobs_posted_date ON fact_jobs(posted_date);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bridge_job_id') THEN
        CREATE INDEX idx_bridge_job_id ON bridge_job_skills(job_id);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_bridge_skill_id') THEN
        CREATE INDEX idx_bridge_skill_id ON bridge_job_skills(skill_id);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dim_skills_name') THEN
        CREATE INDEX idx_dim_skills_name ON dim_skills(skill_name);
    END IF;
END $$;

-- ===== VIEWS - Cho phục vụ báo cáo =====

-- View 1: Top Skills theo tần suất
CREATE OR REPLACE VIEW v_top_skills AS
SELECT 
    ds.skill_name,
    COUNT(bjs.skill_id) as skill_count,
    ROUND(100.0 * COUNT(bjs.skill_id) / (SELECT COUNT(DISTINCT job_id) FROM fact_jobs), 2) as skill_percentage
FROM dim_skills ds
LEFT JOIN bridge_job_skills bjs ON ds.skill_id = bjs.skill_id
GROUP BY ds.skill_id, ds.skill_name
ORDER BY skill_count DESC;

-- View 2: Salary thống kê theo công ty
CREATE OR REPLACE VIEW v_salary_by_company AS
SELECT 
    dc.company_name,
    COUNT(fj.job_id) as job_count,
    ROUND(AVG(fj.salary_min)::NUMERIC, 0) as avg_salary_min,
    ROUND(AVG(fj.salary_max)::NUMERIC, 0) as avg_salary_max,
    ROUND((AVG(fj.salary_min) + AVG(fj.salary_max)) / 2, 0) as avg_salary_midpoint
FROM fact_jobs fj
JOIN dim_companies dc ON fj.company_id = dc.company_id
WHERE fj.salary_min IS NOT NULL AND fj.salary_max IS NOT NULL
GROUP BY dc.company_id, dc.company_name
ORDER BY avg_salary_midpoint DESC;

-- View 3: Job postings theo location
CREATE OR REPLACE VIEW v_jobs_by_location AS
SELECT 
    location,
    COUNT(*) as job_count,
    ROUND(AVG(salary_min)::NUMERIC, 0) as avg_salary_min,
    ROUND(AVG(salary_max)::NUMERIC, 0) as avg_salary_max
FROM fact_jobs
WHERE location IS NOT NULL
GROUP BY location
ORDER BY job_count DESC;

-- View 4: Kỹ năng phổ biến theo vị trí
CREATE OR REPLACE VIEW v_skills_by_location AS
SELECT 
    fj.location,
    ds.skill_name,
    COUNT(bjs.skill_id) as skill_count
FROM fact_jobs fj
JOIN bridge_job_skills bjs ON fj.job_id = bjs.job_id
JOIN dim_skills ds ON bjs.skill_id = ds.skill_id
WHERE fj.location IS NOT NULL
GROUP BY fj.location, ds.skill_id, ds.skill_name
ORDER BY fj.location, skill_count DESC;

-- ===== STORED PROCEDURES - Cho các tác vụ thường xuyên =====

-- Procedure: Insert job với company
CREATE OR REPLACE FUNCTION insert_job_with_company(
    p_job_title VARCHAR,
    p_company_name VARCHAR,
    p_location VARCHAR,
    p_salary_min INT,
    p_salary_max INT,
    p_job_url VARCHAR
)
RETURNS INT AS $$
DECLARE
    v_company_id INT;
    v_job_id INT;
BEGIN
    -- ===== Tìm hoặc tạo company =====
    SELECT company_id INTO v_company_id FROM dim_companies WHERE company_name = p_company_name LIMIT 1;
    
    IF v_company_id IS NULL THEN
        INSERT INTO dim_companies (company_name) VALUES (p_company_name)
        RETURNING company_id INTO v_company_id;
    END IF;
    
    -- ===== Tạo job mới =====
    INSERT INTO fact_jobs (company_id, job_title, location, salary_min, salary_max, job_url, posted_date)
    VALUES (v_company_id, p_job_title, p_location, p_salary_min, p_salary_max, p_job_url, CURRENT_TIMESTAMP)
    RETURNING job_id INTO v_job_id;
    
    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;

