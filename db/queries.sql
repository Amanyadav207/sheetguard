-- ============================================================
-- ETL Analytics & Reporting Queries
-- ============================================================

-- ============================================================
-- 1. QUALITY METRICS - CURRENT STATUS
-- ============================================================

-- Latest ETL run with complete metrics
SELECT 
    id,
    run_timestamp,
    total_rows,
    valid_rows,
    invalid_rows,
    duplicate_emails,
    inserted_rows,
    skipped_rows,
    duration_seconds,
    status,
    ROUND(100.0 * valid_rows / NULLIF(total_rows, 0), 2) AS validity_pct,
    ROUND(100.0 * invalid_rows / NULLIF(total_rows, 0), 2) AS error_rate_pct,
    ROUND(100.0 * duplicate_emails / NULLIF(total_rows, 0), 2) AS duplicate_rate_pct
FROM etl_runs
ORDER BY run_timestamp DESC
LIMIT 1;

-- Top 10 recent runs with metrics
SELECT 
    id,
    DATE(run_timestamp) AS run_date,
    status,
    total_rows,
    valid_rows,
    invalid_rows,
    duplicate_emails,
    inserted_rows,
    skipped_rows,
    duration_seconds,
    ROUND(100.0 * valid_rows / NULLIF(total_rows, 0), 2) AS validity_pct
FROM etl_runs
ORDER BY run_timestamp DESC
LIMIT 10;

-- ============================================================
-- 2. DAILY TRENDS
-- ============================================================

-- Daily quality summary (last 30 days)
SELECT 
    DATE(run_timestamp) AS run_date,
    COUNT(*) AS daily_runs,
    SUM(total_rows) AS total_rows_processed,
    SUM(valid_rows) AS total_valid,
    SUM(invalid_rows) AS total_invalid,
    SUM(duplicate_emails) AS total_duplicates,
    SUM(inserted_rows) AS total_inserted,
    SUM(skipped_rows) AS total_skipped,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
    ROUND(100.0 * SUM(valid_rows) / NULLIF(SUM(total_rows), 0), 2) AS daily_validity_pct,
    ROUND(100.0 * SUM(invalid_rows) / NULLIF(SUM(total_rows), 0), 2) AS daily_error_pct,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '30 days'
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;

-- ============================================================
-- 3. PERFORMANCE METRICS
-- ============================================================

-- Throughput analysis (rows per second)
SELECT 
    id,
    run_timestamp,
    total_rows,
    duration_seconds,
    CASE 
        WHEN duration_seconds > 0 THEN ROUND(total_rows::float / duration_seconds, 2)
        ELSE 0
    END AS rows_per_second,
    CASE 
        WHEN duration_seconds > 0 THEN ROUND(inserted_rows::float / duration_seconds, 2)
        ELSE 0
    END AS inserts_per_second,
    status
FROM etl_runs
WHERE duration_seconds IS NOT NULL
ORDER BY run_timestamp DESC
LIMIT 20;

-- Average performance by hour of day
SELECT 
    EXTRACT(HOUR FROM run_timestamp) AS hour_of_day,
    COUNT(*) AS runs,
    ROUND(AVG(total_rows), 0) AS avg_rows,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
    ROUND(AVG(CASE WHEN duration_seconds > 0 THEN total_rows::float / duration_seconds ELSE 0 END), 2) AS avg_rows_per_sec
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days'
GROUP BY EXTRACT(HOUR FROM run_timestamp)
ORDER BY hour_of_day;

-- ============================================================
-- 4. ERROR ANALYSIS
-- ============================================================

-- Top 15 error reasons
SELECT 
    error_reason,
    COUNT(*) AS frequency,
    COUNT(DISTINCT etl_run_id) AS affected_runs,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM invalid_rows), 2) AS pct_of_total,
    MAX(created_at) AS last_occurrence,
    MIN(created_at) AS first_occurrence,
    DATEDIFF(DAY, MIN(created_at), MAX(created_at)) AS days_recurring
FROM invalid_rows
GROUP BY error_reason
ORDER BY frequency DESC
LIMIT 15;

-- Error distribution by run
SELECT 
    er.id,
    er.run_timestamp,
    er.total_rows,
    er.invalid_rows,
    COUNT(DISTINCT ir.error_reason) AS unique_error_types,
    MAX(ir.error_reason) AS most_common_error,
    ROUND(100.0 * COUNT(*) / NULLIF(er.invalid_rows, 0), 2) AS pct_captured
FROM etl_runs er
LEFT JOIN invalid_rows ir ON ir.etl_run_id = er.id
WHERE er.invalid_rows > 0
GROUP BY er.id, er.run_timestamp, er.total_rows, er.invalid_rows
ORDER BY er.run_timestamp DESC
LIMIT 20;

-- ============================================================
-- 5. DUPLICATE EMAIL ANALYSIS
-- ============================================================

-- Duplicate email trends
SELECT 
    DATE(run_timestamp) AS run_date,
    COUNT(*) AS runs,
    SUM(duplicate_emails) AS total_duplicates,
    ROUND(AVG(CASE WHEN total_rows > 0 THEN 100.0 * duplicate_emails / total_rows ELSE 0 END), 2) AS avg_duplicate_rate_pct,
    MAX(duplicate_emails) AS max_duplicates_in_run,
    MIN(duplicate_emails) AS min_duplicates_in_run
FROM etl_runs
WHERE duplicate_emails > 0
  AND run_timestamp > NOW() - INTERVAL '30 days'
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;

-- Duplicate email percentage by run
SELECT 
    id,
    run_timestamp,
    total_rows,
    duplicate_emails,
    ROUND(100.0 * duplicate_emails / NULLIF(total_rows, 0), 2) AS duplicate_rate_pct,
    status
FROM etl_runs
WHERE duplicate_emails > 0
ORDER BY run_timestamp DESC
LIMIT 20;

-- ============================================================
-- 6. SKIP/SKIP ANALYSIS (Already loaded duplicates)
-- ============================================================

-- Skipped rows (duplicates already in DB) trends
SELECT 
    DATE(run_timestamp) AS run_date,
    COUNT(*) AS runs,
    SUM(skipped_rows) AS total_skipped,
    ROUND(AVG(CASE WHEN inserted_rows + skipped_rows > 0 
        THEN 100.0 * skipped_rows / (inserted_rows + skipped_rows) 
        ELSE 0 END), 2) AS avg_skip_rate_pct,
    MAX(skipped_rows) AS max_skipped_in_run
FROM etl_runs
WHERE skipped_rows > 0
  AND run_timestamp > NOW() - INTERVAL '30 days'
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;

-- ============================================================
-- 7. DATA QUALITY SCORECARD
-- ============================================================

-- Quality metrics comparison - Current vs Historical averages
SELECT 
    'Current (Last Run)' AS period,
    (SELECT total_rows FROM etl_runs ORDER BY id DESC LIMIT 1) AS total_rows,
    (SELECT valid_rows FROM etl_runs ORDER BY id DESC LIMIT 1) AS valid_rows,
    (SELECT invalid_rows FROM etl_runs ORDER BY id DESC LIMIT 1) AS invalid_rows,
    (SELECT duplicate_emails FROM etl_runs ORDER BY id DESC LIMIT 1) AS duplicates,
    ROUND(100.0 * (SELECT valid_rows FROM etl_runs ORDER BY id DESC LIMIT 1) / 
        NULLIF((SELECT total_rows FROM etl_runs ORDER BY id DESC LIMIT 1), 0), 2) AS validity_pct
UNION ALL
SELECT 
    'Last 7 Days Avg',
    ROUND(AVG(total_rows), 0),
    ROUND(AVG(valid_rows), 0),
    ROUND(AVG(invalid_rows), 0),
    ROUND(AVG(duplicate_emails), 0),
    ROUND(100.0 * AVG(CASE WHEN total_rows > 0 THEN valid_rows::float / total_rows ELSE 0 END), 2)
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days'
  AND status IN ('success', 'partial_success')
UNION ALL
SELECT 
    'Last 30 Days Avg',
    ROUND(AVG(total_rows), 0),
    ROUND(AVG(valid_rows), 0),
    ROUND(AVG(invalid_rows), 0),
    ROUND(AVG(duplicate_emails), 0),
    ROUND(100.0 * AVG(CASE WHEN total_rows > 0 THEN valid_rows::float / total_rows ELSE 0 END), 2)
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '30 days'
  AND status IN ('success', 'partial_success');

-- ============================================================
-- 8. STUDENT DATA QUALITY
-- ============================================================

-- Student email distribution
SELECT 
    COUNT(*) AS total_students,
    COUNT(DISTINCT email) AS unique_emails,
    COUNT(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 END) AS students_with_phone,
    COUNT(CASE WHEN year IS NOT NULL THEN 1 END) AS students_with_year,
    COUNT(CASE WHEN department_id IS NOT NULL THEN 1 END) AS students_with_dept,
    COUNT(DISTINCT department_id) AS unique_departments
FROM students;

-- Department distribution
SELECT 
    d.name,
    COUNT(s.id) AS student_count,
    MIN(s.created_at) AS first_added,
    MAX(s.created_at) AS last_added
FROM departments d
LEFT JOIN students s ON s.department_id = d.id
GROUP BY d.id, d.name
ORDER BY student_count DESC;

-- Year distribution
SELECT 
    year,
    COUNT(*) AS student_count
FROM students
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year;

-- ============================================================
-- 9. ETL HEALTH CHECKS
-- ============================================================

-- Recent failures and issues
SELECT 
    id,
    run_timestamp,
    status,
    total_rows,
    invalid_rows,
    error_message,
    duration_seconds
FROM etl_runs
WHERE status = 'failed'
  OR (status = 'partial_success' AND invalid_rows > total_rows * 0.2)
ORDER BY run_timestamp DESC
LIMIT 20;

-- Consistency check - Validate data integrity
SELECT 
    (SELECT COUNT(*) FROM students) AS total_students,
    (SELECT COUNT(DISTINCT email) FROM students) AS unique_emails,
    CASE 
        WHEN (SELECT COUNT(*) FROM students) = (SELECT COUNT(DISTINCT email) FROM students) 
        THEN 'PASS' 
        ELSE 'FAIL - Email duplicates exist' 
    END AS email_uniqueness_check,
    (SELECT COUNT(*) FROM departments) AS total_departments,
    (SELECT COUNT(*) FROM invalid_rows) AS invalid_records_in_queue,
    (SELECT MAX(run_timestamp) FROM etl_runs) AS last_etl_run
FROM (SELECT 1) AS dummy;

-- ============================================================
-- 10. SAMPLE QUERIES FOR DASHBOARDS
-- ============================================================

-- Overview for dashboard - single row
SELECT 
    (SELECT COUNT(*) FROM etl_runs) AS total_runs,
    (SELECT COUNT(*) FROM etl_runs WHERE status = 'success') AS successful_runs,
    (SELECT SUM(total_rows) FROM etl_runs) AS all_rows_processed,
    (SELECT SUM(valid_rows) FROM etl_runs) AS all_valid_rows,
    (SELECT SUM(invalid_rows) FROM etl_runs) AS all_invalid_rows,
    (SELECT SUM(duplicate_emails) FROM etl_runs) AS all_duplicates,
    ROUND(100.0 * (SELECT SUM(valid_rows) FROM etl_runs) / NULLIF((SELECT SUM(total_rows) FROM etl_runs), 0), 2) AS overall_validity_pct,
    (SELECT COUNT(*) FROM students) AS current_student_count,
    (SELECT MAX(run_timestamp) FROM etl_runs) AS last_run_time;
