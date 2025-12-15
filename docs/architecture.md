# ETL Architecture

## Design Principles

### Modularity
Each stage of the ETL pipeline is independent:
- **Extract**: Handles only data retrieval from Google Sheets
- **Transform**: Performs data cleaning and normalization
- **Load**: Manages database writes and idempotency
- **Validate**: Provides reusable validation rules

### Idempotency
The pipeline is designed to be safely rerun without side effects:
- `ON CONFLICT (email) DO NOTHING` prevents duplicate inserts
- Invalid records stored separately for inspection
- Metrics tracked per run for audit trails

### Observability
Every step is logged with context:
- Debug logs for detailed tracing
- Info logs for key milestones
- Error logs with full stack traces
- Metrics stored in `etl_runs` table

### Scalability
Prepared for growth:
- Connection pooling for efficient resource use
- Batch processing to handle large datasets
- Dead-letter queue for error analysis
- Type hints for static analysis

## Data Flow

```
Google Sheets
     ↓
[EXTRACT]
  ├─ Fetch via gspread
  └─ Convert to DataFrame
     ↓
[TRANSFORM]
  ├─ Normalize columns
  ├─ Clean data
  ├─ Deduplicate
  └─ Convert types
     ↓
[VALIDATE]
  ├─ Email validation
  ├─ Name validation
  ├─ Year validation
  ├─ Phone validation
  └─ Department validation
     ↓
         ├─→ VALID → [LOAD]
         │        ├─ Create departments
         │        └─ Insert students (idempotent)
         │             ↓
         │         PostgreSQL (students)
         │
         └─→ INVALID → [DEAD-LETTER]
                      ├─ Store raw JSON
                      └─ Record error reason
                           ↓
                       PostgreSQL (invalid_rows)
     ↓
[METRICS]
  ├─ Record ETL run
  ├─ Track counts
  └─ Log duration
     ↓
 PostgreSQL (etl_runs)
```

## Component Details

### Extract Layer
**File**: `etl/extract.py`

Uses Google Sheets API via `gspread`:
```python
GoogleSheetsExtractor
├─ authenticate()          # Service account auth
├─ extract()               # Full sheet
└─ extract_range()         # Specific range
```

Returns pandas DataFrame for compatibility with transform layer.

**Alternatives**:
- Sheets API directly for more control
- CSV download from Google Drive
- Database replication

### Transform Layer
**File**: `etl/transform.py`

Cleans and normalizes data:
```python
DataTransformer
├─ normalize_columns()     # Column name cleanup
├─ dataframe_to_records()  # Type conversions
└─ deduplicate()           # Email-based dedup
```

**Transformations Applied**:
- Email: lowercase, trim
- Name: trim whitespace
- Year: string → integer
- Phone: trim (optional)
- Department: trim (optional)

### Validate Layer
**File**: `etl/validator.py`

Field-level validation rules:
```python
StudentRecordValidator
├─ EmailValidator
├─ NameValidator
├─ YearValidator
├─ PhoneValidator
└─ DepartmentValidator
```

Each validator is independent and reusable.

### Load Layer
**File**: `etl/load.py`

Handles database writes:
```python
StudentLoader
├─ load_students()         # Idempotent INSERT
├─ _ensure_departments()   # Create missing depts
└─ _load_batch()           # Batch processing

InvalidRowLoader
└─ load_invalid_rows()     # Store errors as JSON
```

**Idempotency Strategy**:
```sql
INSERT INTO students (email, name, ...)
VALUES (...)
ON CONFLICT (email) DO NOTHING;
```

### Database Layer
**File**: `db/connection.py`

Connection pooling and helper methods:
```python
DatabaseConnection
├─ initialize()            # Create pool
├─ get_connection()        # Context manager
├─ get_cursor()            # Cursor context
├─ execute_query()         # SELECT
├─ execute_update()        # INSERT/UPDATE/DELETE
└─ execute_many()          # Batch operations
```

Singleton pattern ensures single pool per process.

### Orchestration Layer
**File**: `etl/run_etl.py`

Coordinates the entire pipeline:
```python
ETLOrchestrator
├─ run()                   # Main entry point
├─ _initialize_database()  # Setup connection
├─ _create_etl_run_record() # Start metrics
├─ _execute_pipeline()     # Coordinate steps
└─ _finalize_etl_run()     # Record metrics
```

## Error Handling Strategy

### At Each Layer

**Extract**:
- Catch API timeouts and auth errors
- Retry with exponential backoff
- Log and fail fast if unrecoverable

**Transform**:
- Skip null values gracefully
- Convert types with fallback to string
- Log type conversion warnings

**Validate**:
- Capture detailed error messages
- Separate valid from invalid records
- No exceptions thrown

**Load**:
- Batch errors are logged but don't stop pipeline
- Dead-letter queue captures all failures
- Metrics updated on all outcomes

**Orchestration**:
- Catches all exceptions from stages
- Updates ETL run status to 'failed'
- Logs full stack trace
- Cleans up database connections

### Dead-Letter Queue Pattern

Invalid records stored in `invalid_rows` table:

```json
{
  "id": 123,
  "etl_run_id": 1,
  "raw_data": {
    "email": "invalid",
    "name": "John Doe",
    "year": "abc"
  },
  "error_reason": "email: Invalid email format: invalid",
  "row_number": 5,
  "created_at": "2025-12-15T14:23:47Z"
}
```

Allows for:
- Post-run inspection
- Root cause analysis
- Reprocessing with fixes
- Compliance/audit trails

## Performance Considerations

### Connection Pooling
- Reuses connections across requests
- Reduces handshake overhead
- Configurable pool size (default: 1-10)

### Batch Processing
- Default batch size: 100 records
- Configurable via `BATCH_SIZE` env var
- Reduces transaction overhead

### Indexing
- Primary key on student id
- Unique constraint on email
- Index on email for lookups
- Index on department_id for joins

### Query Optimization
```sql
-- Fast email lookups
CREATE INDEX idx_students_email ON students(email);

-- Fast department filtering
CREATE INDEX idx_students_department_id ON students(department_id);

-- Fast invalid row filtering
CREATE INDEX idx_invalid_rows_run_id ON invalid_rows(etl_run_id);
```

## Configuration Management

**File**: `config/settings.py`

All configuration via environment variables:
```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=postgres
DB_PASSWORD=***

# Google Sheets
GOOGLE_SHEET_ID=***
GOOGLE_CREDENTIALS_PATH=credentials.json

# ETL
BATCH_SIZE=100
MAX_RETRIES=3

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/etl.log
```

**Benefits**:
- No hardcoded credentials
- Environment-specific config
- Docker/Kubernetes friendly
- Secrets management compatible

## Testing Strategy

### Unit Tests
Test each validator independently:
```python
def test_email_validator():
    validator = EmailValidator()
    assert validator.validate("user@example.com") == (True, None)
    assert validator.validate("invalid")[0] == False
```

### Integration Tests
Test data flow end-to-end with mock data:
```python
def test_etl_pipeline():
    df = pd.DataFrame([...])
    valid, invalid = validate_and_transform(df)
    assert len(valid) > 0
    assert len(invalid) == 0
```

### E2E Tests
Test against real database and Google Sheets (staging environment).

## Security Considerations

1. **Credentials**
   - Service account JSON in `.env`
   - Never commit credentials
   - Use environment variables in production

2. **Database**
   - SSL/TLS for connections
   - Database user with minimal permissions
   - Connection pooling prevents connection exhaustion

3. **Data Validation**
   - All external data validated before insert
   - SQL injection prevention via parameterized queries
   - Type checking before database operations

4. **Logging**
   - Sensitive data (passwords, API keys) never logged
   - Log files stored securely
   - Rotation and retention policies

## Deployment

### Local Development
```bash
python -m etl.run_etl
```

### Docker
```bash
docker build -t etl-pipeline .
docker run --env-file .env etl-pipeline
```

### Kubernetes
```bash
kubectl create configmap etl-config --from-env-file=.env
kubectl apply -f etl-job.yaml
```

### Scheduled (APScheduler)
```python
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
scheduler.add_job(orchestrator.run, 'cron', hour=2)  # Daily at 2 AM
scheduler.start()
```

## Monitoring

### Key Metrics
- Total rows processed
- Valid vs invalid ratio
- Insert vs skip count
- Duration per run
- Error rate

### Queries
```sql
-- Latest ETL run status
SELECT * FROM etl_runs ORDER BY run_timestamp DESC LIMIT 1;

-- Invalid records from today
SELECT * FROM invalid_rows 
WHERE created_at > NOW() - INTERVAL '1 day'
ORDER BY created_at DESC;

-- Success rate last 7 days
SELECT 
  COUNT(*) total_runs,
  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) successful
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days';
```
