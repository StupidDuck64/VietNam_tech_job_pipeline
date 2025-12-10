-- ===== VN IT Job Analytics - Sample Queries =====
-- Các câu query mẫu cho phân tích dữ liệu
-- Author: Data Engineering Team
-- Date: December 2025 (Updated)

-- ===== 1. TOP SKILLS IN THE MARKET =====
-- Kỹ năng nào được tìm kiếm nhiều nhất hiện nay?
SELECT 
    ds.skill_name,
    COUNT(bjs.skill_id) as job_count,
    ROUND(100.0 * COUNT(bjs.skill_id) / (SELECT COUNT(DISTINCT job_id) FROM fact_jobs), 2) as percentage
FROM dim_skills ds
LEFT JOIN bridge_job_skills bjs ON ds.skill_id = bjs.skill_id
GROUP BY ds.skill_id, ds.skill_name
ORDER BY job_count DESC
LIMIT 20;

-- ===== 2. SALARY ANALYSIS BY LOCATION =====
-- Mức lương trung bình theo địa điểm?
SELECT 
    location,
    COUNT(*) as total_jobs,
    ROUND(AVG(salary_min)::NUMERIC, 0) as avg_min_salary,
    ROUND(AVG(salary_max)::NUMERIC, 0) as avg_max_salary,
    ROUND((AVG(salary_min) + AVG(salary_max)) / 2, 0) as avg_midpoint
FROM fact_jobs
WHERE location IS NOT NULL 
    AND salary_min IS NOT NULL 
    AND salary_max IS NOT NULL
GROUP BY location
ORDER BY avg_midpoint DESC;

-- ===== 3. COMPANY WITH MOST JOB POSTINGS =====
-- Công ty nào đang tuyển dụng nhiều nhất?
SELECT 
    dc.company_name,
    COUNT(fj.job_id) as job_count,
    COUNT(DISTINCT fj.location) as locations_count,
    ROUND(AVG((fj.salary_min + fj.salary_max) / 2)::NUMERIC, 0) as avg_salary
FROM fact_jobs fj
JOIN dim_companies dc ON fj.company_id = dc.company_id
GROUP BY dc.company_id, dc.company_name
ORDER BY job_count DESC
LIMIT 15;

-- ===== 4. SKILL COMBINATION ANALYSIS =====
-- Những kỹ năng thường được yêu cầu cùng nhau?
SELECT 
    ds1.skill_name as skill_1,
    ds2.skill_name as skill_2,
    COUNT(DISTINCT bjs1.job_id) as co_occurrence_count
FROM bridge_job_skills bjs1
JOIN bridge_job_skills bjs2 ON bjs1.job_id = bjs2.job_id AND bjs1.skill_id < bjs2.skill_id
JOIN dim_skills ds1 ON bjs1.skill_id = ds1.skill_id
JOIN dim_skills ds2 ON bjs2.skill_id = ds2.skill_id
GROUP BY bjs1.skill_id, bjs2.skill_id, ds1.skill_name, ds2.skill_name
ORDER BY co_occurrence_count DESC
LIMIT 20;

-- ===== 5. JOB TITLE DISTRIBUTION =====
-- Vị trí việc làm nào có nhiều job postings?
SELECT 
    job_title,
    COUNT(*) as job_count,
    ROUND(AVG((salary_min + salary_max) / 2)::NUMERIC, 0) as avg_salary,
    COUNT(DISTINCT company_id) as unique_companies
FROM fact_jobs
GROUP BY job_title
ORDER BY job_count DESC
LIMIT 20;

-- ===== 6. LOCATION POPULARITY =====
-- Địa điểm nào có lượng job nhiều nhất?
SELECT 
    location,
    COUNT(*) as job_count,
    COUNT(DISTINCT company_id) as unique_companies,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_jobs), 2) as percentage
FROM fact_jobs
WHERE location IS NOT NULL
GROUP BY location
ORDER BY job_count DESC
LIMIT 15;

-- ===== 7. SALARY RANGE DISTRIBUTION =====
-- Phân bố range lương như thế nào?
SELECT 
    CASE 
        WHEN salary_max IS NULL THEN 'Not disclosed'
        WHEN salary_max < 1000 THEN 'Under 1000'
        WHEN salary_max < 2000 THEN '1000 - 2000'
        WHEN salary_max < 3000 THEN '2000 - 3000'
        WHEN salary_max < 4000 THEN '3000 - 4000'
        WHEN salary_max < 5000 THEN '4000 - 5000'
        ELSE 'Above 5000'
    END as salary_range,
    COUNT(*) as job_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_jobs WHERE salary_max IS NOT NULL), 2) as percentage
FROM fact_jobs
GROUP BY salary_range
ORDER BY job_count DESC;

-- ===== 8. DATA QUALITY REPORT =====
-- Chất lượng dữ liệu như thế nào?
SELECT 
    'Total Jobs' as metric,
    COUNT(*)::TEXT as value
FROM fact_jobs
UNION ALL
SELECT 
    'Jobs with Salary Info',
    COUNT(*)::TEXT
FROM fact_jobs
WHERE salary_min IS NOT NULL OR salary_max IS NOT NULL
UNION ALL
SELECT 
    'Jobs with Location',
    COUNT(*)::TEXT
FROM fact_jobs
WHERE location IS NOT NULL
UNION ALL
SELECT 
    'Unique Companies',
    COUNT(DISTINCT company_id)::TEXT
FROM fact_jobs
UNION ALL
SELECT 
    'Unique Locations',
    COUNT(DISTINCT location)::TEXT
FROM fact_jobs
WHERE location IS NOT NULL
UNION ALL
SELECT 
    'Unique Skills Mentioned',
    COUNT(DISTINCT skill_id)::TEXT
FROM bridge_job_skills;

-- ===== 9. SKILLS REQUIRED FOR SPECIFIC JOB TITLES =====
-- Kỹ năng cần thiết cho vị trí Data Engineer là gì?
SELECT 
    ds.skill_name,
    COUNT(bjs.skill_id) as required_count,
    ROUND(100.0 * COUNT(bjs.skill_id) / 
        (SELECT COUNT(DISTINCT fj.job_id) 
         FROM fact_jobs fj 
         WHERE fj.job_title ILIKE '%data engineer%'), 2) as percentage_required
FROM fact_jobs fj
JOIN bridge_job_skills bjs ON fj.job_id = bjs.job_id
JOIN dim_skills ds ON bjs.skill_id = ds.skill_id
WHERE fj.job_title ILIKE '%data engineer%'
GROUP BY ds.skill_id, ds.skill_name
ORDER BY required_count DESC;

-- ===== 10. RECENT JOB POSTINGS =====
-- Công việc được đăng tải trong vòng 7 ngày gần đây?
SELECT 
    job_title,
    dc.company_name,
    location,
    salary_min,
    salary_max,
    posted_date
FROM fact_jobs fj
JOIN dim_companies dc ON fj.company_id = dc.company_id
WHERE posted_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY posted_date DESC
LIMIT 20;

-- ===== 11. COMPANY SALARY BENCHMARK =====
-- Công ty nào trả lương cao nhất?
SELECT 
    dc.company_name,
    COUNT(fj.job_id) as job_count,
    ROUND(MAX(fj.salary_max)::NUMERIC, 0) as max_salary,
    ROUND(AVG((fj.salary_min + fj.salary_max) / 2)::NUMERIC, 0) as avg_salary,
    ROUND(MIN(fj.salary_min)::NUMERIC, 0) as min_salary
FROM fact_jobs fj
JOIN dim_companies dc ON fj.company_id = dc.company_id
WHERE fj.salary_min IS NOT NULL AND fj.salary_max IS NOT NULL
GROUP BY dc.company_id, dc.company_name
ORDER BY avg_salary DESC
LIMIT 20;

-- ===== 12. EXPORT JOBS WITH ALL SKILLS =====
-- Export danh sách job với tất cả kỹ năng của chúng
SELECT 
    fj.job_id,
    fj.job_title,
    dc.company_name,
    fj.location,
    fj.salary_min,
    fj.salary_max,
    STRING_AGG(ds.skill_name, ', ' ORDER BY ds.skill_name) as required_skills
FROM fact_jobs fj
JOIN dim_companies dc ON fj.company_id = dc.company_id
LEFT JOIN bridge_job_skills bjs ON fj.job_id = bjs.job_id
LEFT JOIN dim_skills ds ON bjs.skill_id = ds.skill_id
GROUP BY fj.job_id, fj.job_title, dc.company_name, fj.location, fj.salary_min, fj.salary_max
ORDER BY fj.posted_date DESC
LIMIT 100;

-- ===== 13. MONITORING QUERY - Last Scrape Status =====
-- Tình trạng scrape gần đây nhất?
SELECT 
    MAX(scraped_at) as last_scrape_time,
    COUNT(*) as jobs_added,
    ROUND(AVG(data_quality_score), 2) as avg_quality
FROM fact_jobs
WHERE scraped_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- ===== 14. ANOMALY DETECTION - Jobs with Missing Data =====
-- Công việc nào thiếu dữ liệu (missing fields)?
SELECT 
    job_id,
    job_title,
    CASE WHEN salary_min IS NULL THEN 'Missing salary_min' ELSE '' END as missing_field_1,
    CASE WHEN location IS NULL THEN 'Missing location' ELSE '' END as missing_field_2,
    CASE WHEN company_id IS NULL THEN 'Missing company' ELSE '' END as missing_field_3
FROM fact_jobs
WHERE salary_min IS NULL 
   OR location IS NULL 
   OR company_id IS NULL
LIMIT 20;
