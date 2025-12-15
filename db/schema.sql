-- ETL Database Schema
-- NeonDB (PostgreSQL-compatible)

-- Departments table
CREATE TABLE IF NOT EXISTS departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Students table
CREATE TABLE IF NOT EXISTS students (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    department_id INTEGER REFERENCES departments(id),
    year INTEGER CHECK (year >= 1 AND year <= 4),
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_year CHECK (year IS NULL OR (year >= 1 AND year <= 4))
);

-- Create index on email for faster lookups
CREATE INDEX IF NOT EXISTS idx_students_email ON students(email);

-- Create index on department for filtering
CREATE INDEX IF NOT EXISTS idx_students_department_id ON students(department_id);

-- ETL Runs table (tracks metrics)
CREATE TABLE IF NOT EXISTS etl_runs (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_rows INTEGER NOT NULL,
    valid_rows INTEGER NOT NULL,
    invalid_rows INTEGER NOT NULL,
    inserted_rows INTEGER NOT NULL,
    updated_rows INTEGER NOT NULL,
    duration_seconds DECIMAL(10, 2),
    status VARCHAR(50) NOT NULL CHECK (status IN ('in_progress', 'success', 'partial_success', 'failed')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Invalid Rows (Dead-Letter Queue)
CREATE TABLE IF NOT EXISTS invalid_rows (
    id SERIAL PRIMARY KEY,
    etl_run_id INTEGER REFERENCES etl_runs(id),
    raw_data JSONB NOT NULL,
    error_reason TEXT NOT NULL,
    row_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for querying invalid rows by run
CREATE INDEX IF NOT EXISTS idx_invalid_rows_run_id ON invalid_rows(etl_run_id);

-- Create index on created_at for date range queries
CREATE INDEX IF NOT EXISTS idx_etl_runs_timestamp ON etl_runs(run_timestamp);

-- Enhanced ETL Runs table with additional metrics
ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS duplicate_emails INTEGER DEFAULT 0;
ALTER TABLE etl_runs ADD COLUMN IF NOT EXISTS skipped_rows INTEGER DEFAULT 0;

-- ============================================================
-- VIEWS FOR ANALYTICS AND TREND ANALYSIS
-- ============================================================

-- Quality metrics view - Latest 100 runs
CREATE OR REPLACE VIEW vw_etl_quality_metrics AS
SELECT 
    id AS run_id,
    run_timestamp,
    total_rows,
    valid_rows,
    invalid_rows,
    duplicate_emails,
    inserted_rows,
    skipped_rows,
    duration_seconds,
    status,
    CASE 
        WHEN total_rows = 0 THEN 0
        ELSE ROUND(100.0 * valid_rows / total_rows, 2)
    END AS validity_rate_pct,
    CASE 
        WHEN total_rows = 0 THEN 0
        ELSE ROUND(100.0 * invalid_rows / total_rows, 2)
    END AS error_rate_pct,
    CASE 
        WHEN total_rows = 0 THEN 0
        ELSE ROUND(100.0 * duplicate_emails / total_rows, 2)
    END AS duplicate_rate_pct,
    CASE 
        WHEN inserted_rows = 0 THEN 0
        ELSE ROUND(100.0 * skipped_rows / (inserted_rows + skipped_rows), 2)
    END AS skip_rate_pct
FROM etl_runs
ORDER BY run_timestamp DESC
LIMIT 100;

-- Daily quality summary view
CREATE OR REPLACE VIEW vw_daily_quality_summary AS
SELECT 
    DATE(run_timestamp) AS run_date,
    COUNT(*) AS total_runs,
    SUM(total_rows) AS total_rows_processed,
    SUM(valid_rows) AS total_valid_rows,
    SUM(invalid_rows) AS total_invalid_rows,
    SUM(duplicate_emails) AS total_duplicates,
    SUM(inserted_rows) AS total_inserted,
    SUM(skipped_rows) AS total_skipped,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
    CASE 
        WHEN SUM(total_rows) = 0 THEN 0
        ELSE ROUND(100.0 * SUM(valid_rows) / SUM(total_rows), 2)
    END AS daily_validity_rate_pct,
    CASE 
        WHEN SUM(total_rows) = 0 THEN 0
        ELSE ROUND(100.0 * SUM(invalid_rows) / SUM(total_rows), 2)
    END AS daily_error_rate_pct
FROM etl_runs
WHERE status IN ('success', 'partial_success')
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;

-- Error breakdown view - Top error reasons
CREATE OR REPLACE VIEW vw_error_breakdown AS
SELECT 
    error_reason,
    COUNT(*) AS frequency,
    COUNT(DISTINCT etl_run_id) AS affected_runs,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM invalid_rows), 2) AS pct_of_total,
    MAX(created_at) AS last_occurrence,
    MIN(created_at) AS first_occurrence
FROM invalid_rows
GROUP BY error_reason
ORDER BY frequency DESC;

-- Data quality trend view - 7-day rolling window
CREATE OR REPLACE VIEW vw_quality_trends_7day AS
SELECT 
    DATE(run_timestamp) AS run_date,
    COUNT(*) AS daily_runs,
    ROUND(AVG(CASE WHEN total_rows = 0 THEN 0 ELSE 100.0 * valid_rows / total_rows END), 2) AS avg_validity_pct,
    ROUND(AVG(CASE WHEN total_rows = 0 THEN 0 ELSE 100.0 * invalid_rows / total_rows END), 2) AS avg_error_pct,
    ROUND(AVG(CASE WHEN total_rows = 0 THEN 0 ELSE 100.0 * duplicate_emails / total_rows END), 2) AS avg_duplicate_pct,
    MIN(CASE WHEN total_rows = 0 THEN 0 ELSE 100.0 * valid_rows / total_rows END) AS min_validity_pct,
    MAX(CASE WHEN total_rows = 0 THEN 0 ELSE 100.0 * valid_rows / total_rows END) AS max_validity_pct
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days'
  AND status IN ('success', 'partial_success')
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;

-- Performance metrics view
CREATE OR REPLACE VIEW vw_performance_metrics AS
SELECT 
    id AS run_id,
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
LIMIT 50;

-- Data quality scorecard view
CREATE OR REPLACE VIEW vw_quality_scorecard AS
SELECT 
    'Last Run' AS metric,
    (SELECT run_timestamp FROM etl_runs ORDER BY id DESC LIMIT 1) AS timestamp,
    (SELECT ROUND(100.0 * valid_rows / NULLIF(total_rows, 0), 2) FROM etl_runs ORDER BY id DESC LIMIT 1) AS score
UNION ALL
SELECT 
    'Last 7 Days Average',
    NOW(),
    ROUND(AVG(100.0 * valid_rows / NULLIF(total_rows, 0)), 2)
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days'
  AND status IN ('success', 'partial_success')
UNION ALL
SELECT 
    'Last 30 Days Average',
    NOW(),
    ROUND(AVG(100.0 * valid_rows / NULLIF(total_rows, 0)), 2)
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '30 days'
  AND status IN ('success', 'partial_success');
