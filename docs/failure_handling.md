# Failure Handling & Recovery Guide

## Overview

The ETL system is designed to handle failures gracefully at each stage while preserving data integrity and providing visibility for recovery.

## Failure Types & Mitigation

### Extract Phase Failures

#### Google Sheets API Unavailable
**Symptoms**: Connection timeout, 503 Service Unavailable  
**Mitigation**:
- Retry with exponential backoff
- Default: 3 retries with 2s, 4s, 8s delays
- Log all retry attempts

**Code**:
```python
from etl.extract import GoogleSheetsExtractor

extractor = GoogleSheetsExtractor(settings.GOOGLE_CREDENTIALS_PATH)

# Implement retry logic
for attempt in range(settings.MAX_RETRIES):
    try:
        df = extractor.extract(sheet_id, sheet_name)
        break
    except Exception as e:
        if attempt < settings.MAX_RETRIES - 1:
            wait_time = 2 ** (attempt + 1)
            logger.warning(f"Extract failed, retrying in {wait_time}s: {e}")
            time.sleep(wait_time)
        else:
            logger.error(f"Extract failed after {settings.MAX_RETRIES} attempts")
            raise
```

#### Authentication Failure
**Symptoms**: `Credentials file not found`, `Invalid credentials`  
**Recovery**:
1. Verify `credentials.json` exists and is valid JSON
2. Verify service account email has access to sheet
3. Check API is enabled in Google Cloud Console

**Diagnostic**:
```bash
# Test credentials
python -c "import gspread; from google.oauth2.service_account import Credentials; Credentials.from_service_account_file('credentials.json')"
```

#### Empty or Malformed Sheet
**Symptoms**: Empty DataFrame returned, missing columns  
**Recovery**:
1. Verify sheet name is correct
2. Check sheet has headers in first row
3. Verify data exists in expected range

**Diagnostic Query**:
```python
from etl.extract import GoogleSheetsExtractor

extractor = GoogleSheetsExtractor(settings.GOOGLE_CREDENTIALS_PATH)
df = extractor.extract(sheet_id, sheet_name)
print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(df.head())
```

### Transform Phase Failures

#### Type Conversion Errors
**Symptoms**: String values cannot convert to integer, invalid email format  
**Mitigation**: Treat as validation error, not fatal
```python
try:
    year = int(float(str(value).strip()))
except (ValueError, TypeError):
    # Handled by validator, sent to dead-letter queue
    error = f"Year must be integer, got: {value}"
    invalid_records.append({"record": record, "error_reason": error})
```

#### Memory Issues with Large Datasets
**Symptoms**: MemoryError with large sheets  
**Solutions**:
1. Use range-based extraction:
```python
# Extract in chunks
ranges = ["A1:F1000", "A1001:F2000", "A2001:F3000"]
dfs = [extractor.extract_range(sheet_id, sheet_name, r) for r in ranges]
df = pd.concat(dfs, ignore_index=True)
```

2. Process in batches:
```python
def process_large_sheet(sheet_id, batch_size=10000):
    df = fetch_google_sheet(settings)
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i+batch_size]
        valid, invalid = validate_and_transform(batch_df)
        load_students(valid)
        load_invalid_rows(invalid, etl_run_id)
```

### Load Phase Failures

#### Database Connection Error
**Symptoms**: `psycopg2.OperationalError: connection refused`  
**Recovery**:
```python
from db.connection import DatabaseConnection

try:
    DatabaseConnection.initialize(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
except Exception as e:
    logger.error(f"Database connection failed: {e}")
    # Check database is running
    # Verify network connectivity
    # Verify credentials
    raise
```

**Diagnostic**:
```bash
# Test PostgreSQL connection
psql -h {host} -p {port} -U {user} -d {database} -c "SELECT 1"
```

#### Unique Constraint Violation
**Symptoms**: Duplicate email error on insert  
**Mitigation**: Already handled by `ON CONFLICT DO NOTHING`
```sql
INSERT INTO students (email, name, ...)
VALUES (...)
ON CONFLICT (email) DO NOTHING;
```
This is idempotent—rerunning the same ETL is safe.

#### Invalid Data Type for Column
**Symptoms**: `psycopg2.DataError: value too long for type character`  
**Prevention**: Validate data length before insert
```python
assert len(email) <= 255, f"Email too long: {email}"
assert len(name) <= 255, f"Name too long: {name}"
```

#### Batch Insert Partial Failure
**Symptoms**: Some rows in batch succeed, some fail  
**Current Behavior**:
- Invalid records already separated in transform
- Only valid records reach load layer
- Each batch transaction commits independently

**Log Example**:
```
2025-12-15 14:23:47 | INFO  | etl.load | Batch loaded: 98 inserted, 2 skipped
```

### Orchestration Phase Failures

#### ETL Run Crashes Mid-Pipeline
**Symptoms**: Process killed, database connection lost  
**Recovery**:
```python
def run(self) -> bool:
    try:
        # ... pipeline steps ...
        return True
    except Exception as e:
        # Always update status
        self._finalize_etl_run("failed", str(e))
        return False
    finally:
        # Always close connections
        DatabaseConnection.close_all()
```

#### Metrics Recording Fails
**Symptoms**: Can't update `etl_runs` table  
**Impact**: Metrics not recorded, but data loaded successfully
**Recovery**:
```python
try:
    self._finalize_etl_run("success")
except Exception as e:
    logger.error(f"Failed to record metrics: {e}")
    # Don't fail the entire pipeline for metrics
    # Data already loaded
```

## Error Scenarios & Resolution

### Scenario 1: High Invalid Record Rate (>20%)

**Investigation**:
```sql
-- Find error pattern
SELECT error_reason, COUNT(*) count
FROM invalid_rows
WHERE etl_run_id IN (
  SELECT id FROM etl_runs 
  WHERE run_timestamp > NOW() - INTERVAL '24 hours'
)
GROUP BY error_reason
ORDER BY count DESC;

-- Check example records
SELECT raw_data, error_reason
FROM invalid_rows
WHERE etl_run_id = ? 
LIMIT 10;
```

**Solutions**:
1. Schema changed in Google Sheet (missing columns)
   - Update extraction logic
   - Map new column names

2. Data quality degraded (incomplete entries)
   - Contact data provider
   - Implement data cleaning scripts
   - Update validation rules if appropriate

3. Validation rules too strict
   - Review error messages
   - Adjust field length/format rules
   - Document new acceptable formats

### Scenario 2: Duplicate Email Inserts

**Why It Happens**: Multiple records with same email in batch

**Current Behavior**: 
- Deduplicated in transform (keeps first)
- If database already has email, ON CONFLICT skips

**Verify**:
```sql
-- Check for duplicates
SELECT email, COUNT(*) count
FROM students
GROUP BY email
HAVING COUNT(*) > 1;

-- Should return nothing (UNIQUE constraint enforced)
```

### Scenario 3: Partial ETL Success

**Example Run Status**:
```
{
  "status": "partial_success",
  "total_rows": 1000,
  "valid_rows": 980,
  "invalid_rows": 20,
  "inserted_rows": 978,
  "error_message": null
}
```

**Analysis**:
- 2 rows skipped (duplicates in database)
- 20 rows invalid (errors in transform)
- 978 rows successfully inserted

**Action**: Review invalid_rows table, fix data, rerun

### Scenario 4: Google Sheets API Rate Limit

**Symptoms**: `429 Too Many Requests`  
**Prevention**:
```python
import time

# Add delay between requests
time.sleep(1)  # 1 second between API calls

# Or use batch API
batch_get([range1, range2, range3])  # Single request for 3 ranges
```

**Mitigation**:
```python
# Implement circuit breaker
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
```

## Recovery Procedures

### Full Pipeline Retry
```bash
# Rerun entire pipeline
python -m etl.run_etl

# Logs will show:
# 2025-12-15 14:25:00 | INFO | Extract complete: 1000 rows
# 2025-12-15 14:25:01 | INFO | Transform complete: 985 valid, 15 invalid
# 2025-12-15 14:25:02 | INFO | Load complete: 980 inserted, 5 skipped
```

### Reprocess Invalid Records
```python
import json
import pandas as pd
from db.connection import DatabaseConnection
from etl.transform import validate_and_transform
from etl.load import load_students

# Get invalid records
query = """
SELECT raw_data FROM invalid_rows 
WHERE etl_run_id = ? AND created_at > NOW() - INTERVAL '24 hours'
"""

invalid_data = DatabaseConnection.execute_query(query, (etl_run_id,))
records = [json.loads(row[0]) for row in invalid_data]

# Manually fix records (example)
for record in records:
    if 'email' in record and '@' not in record['email']:
        # Guess domain or ask user
        record['email'] = record['email'] + '@example.com'

# Revalidate and load
df = pd.DataFrame(records)
valid, still_invalid = validate_and_transform(df)

if valid:
    load_students(valid)
    logger.info(f"Recovered {len(valid)} records")
```

### Clear and Restart
```sql
-- WARNING: Destructive!
-- Only in development or staging

DELETE FROM students;
DELETE FROM invalid_rows;
DELETE FROM etl_runs;
DELETE FROM departments;

-- Restart pipeline
```

## Monitoring & Alerts

### Key Health Checks
```sql
-- Last ETL run status
SELECT 
  id,
  status,
  total_rows,
  valid_rows,
  ROUND(100.0 * valid_rows / total_rows, 2) validity_pct,
  duration_seconds,
  run_timestamp
FROM etl_runs
ORDER BY run_timestamp DESC
LIMIT 1;

-- Pipeline failure rate (last 7 days)
SELECT 
  CASE WHEN status = 'failed' THEN 'Failed' ELSE 'Succeeded' END,
  COUNT(*) count,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM etl_runs WHERE run_timestamp > NOW() - INTERVAL '7 days'), 2) pct
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days'
GROUP BY status;

-- Most common errors (last 24 hours)
SELECT 
  error_reason,
  COUNT(*) frequency
FROM invalid_rows
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY error_reason
ORDER BY frequency DESC
LIMIT 5;
```

### Alert Thresholds
```python
def check_health():
    latest = DatabaseConnection.execute_query(
        "SELECT status, valid_rows, total_rows FROM etl_runs ORDER BY id DESC LIMIT 1"
    )[0]
    
    status, valid_rows, total_rows = latest
    validity_pct = (valid_rows / total_rows) * 100 if total_rows > 0 else 0
    
    if status == 'failed':
        alert("ETL pipeline failed")
    elif validity_pct < 90:
        alert(f"Data quality low: {validity_pct}%")
    elif validity_pct < 80:
        alert(f"CRITICAL: Data quality degraded: {validity_pct}%")
```

## Logging Best Practices

### Enable Debug Logging
```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Now see detailed trace
logger.debug("Processing record: %s", record)
logger.info("Batch inserted: %d rows", count)
logger.warning("Duplicate found: %s", email)
logger.error("Insert failed: %s", error, exc_info=True)
```

### Log Locations
- **Console**: Real-time monitoring
- **File** (`logs/etl.log`): Historical audit trail
- **Database** (`etl_runs`, `invalid_rows`): Searchable metrics

## Automation & Scheduled Runs

### APScheduler Setup
```python
from apscheduler.schedulers.background import BackgroundScheduler

def scheduled_etl():
    orchestrator = ETLOrchestrator(settings)
    orchestrator.run()

scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_etl, 'cron', hour=2, minute=0)  # Daily at 2 AM
scheduler.start()

# Check health every hour
scheduler.add_job(check_health, 'interval', hours=1)
```

### Alerting Integration
```python
import smtplib
from email.mime.text import MIMEText

def send_alert(subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = 'etl@example.com'
    msg['To'] = 'admin@example.com'
    
    with smtplib.SMTP('localhost') as server:
        server.send_message(msg)

# Usage
if validity_pct < 80:
    send_alert(
        "ETL Quality Alert",
        f"Data validity dropped to {validity_pct}%"
    )
```

## Conclusion

The ETL system is designed to be:
- **Resilient**: Handles failures without data loss
- **Observable**: Every step logged with metrics
- **Recoverable**: Clear procedures for fixing issues
- **Auditable**: Dead-letter queue preserves problem data

Always check logs and invalid_rows table first when investigating issues.
