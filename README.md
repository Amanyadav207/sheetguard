# ETL Pipeline: Google Sheets → PostgreSQL/NeonDB

A production-grade ETL system that ingests student data from Google Sheets into PostgreSQL with comprehensive data validation, error handling, and metrics tracking.

## Features

✅ **Modular Architecture** - Clean separation of extract, transform, load  
✅ **Schema-Aware ETL** - Type-safe operations with validation  
✅ **Idempotent Inserts** - Safe for repeated runs  
✅ **Data Quality** - Field-level validation and type conversion  
✅ **Dead-Letter Queue** - Invalid rows preserved as JSON for inspection  
✅ **Metrics & Logging** - Track ETL performance and issues  
✅ **Connection Pooling** - Efficient database resource management  
✅ **Type Hints** - Full type annotations for IDE support  

## Project Structure

```
project_root/
├── etl/
│   ├── extract.py          # Google Sheets → DataFrame
│   ├── transform.py        # Validation & normalization
│   ├── load.py             # INSERT with idempotency
│   ├── validator.py        # Field-level validators
│   └── run_etl.py          # Pipeline orchestrator
├── db/
│   ├── connection.py       # PostgreSQL connection pooling
│   ├── schema.sql          # Database schema
│   └── queries.sql         # Analytics queries (future)
├── config/
│   └── settings.py         # Configuration from env variables
├── logs/
│   └── etl.log             # Execution logs
├── docs/
│   ├── architecture.md
│   ├── data_quality.md
│   └── failure_handling.md
├── .env.example
├── requirements.txt
└── README.md
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Environment

Copy and configure `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
DB_HOST=your-neondb-host.neon.tech
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_user
DB_PASSWORD=your_password
GOOGLE_SHEET_ID=your_sheet_id_here
GOOGLE_CREDENTIALS_PATH=credentials.json
SHEET_NAME=Students
```

### 3. Set Up Database

Create the schema:

```bash
psql -h your-host -U your-user -d your-db -f db/schema.sql
```

Or use Python:

```python
from db.connection import DatabaseConnection
from config.settings import Settings

settings = Settings()
DatabaseConnection.initialize(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    database=settings.DB_NAME,
    user=settings.DB_USER,
    password=settings.DB_PASSWORD,
)

with open('db/schema.sql', 'r') as f:
    sql = f.read()
    with DatabaseConnection.get_cursor() as cursor:
        cursor.execute(sql)
```

### 4. Set Up Google Sheets API

1. Create a Google Cloud project
2. Enable Google Sheets API
3. Create a service account and download JSON credentials
4. Place credentials file as `credentials.json` in project root
5. Share your Google Sheet with the service account email

### 5. Run ETL Pipeline

```bash
python -m etl.run_etl
```

## How It Works

### Extract
- Uses `gspread` to fetch data from Google Sheets
- Returns pandas DataFrame with all sheet data
- Supports range-based extraction for large sheets

### Transform
- Normalizes column names (lowercase with underscores)
- Cleans data:
  - Email: lowercase, trim whitespace
  - Name: trim whitespace
  - Year: convert to integer (1-4)
  - Phone: optional, trim whitespace
  - Department: optional, trim whitespace
- Deduplicates by email (keeps first occurrence)

### Validate
- **Email**: Valid format, max 255 chars
- **Name**: 2-255 chars, letters/spaces/hyphens/apostrophes only
- **Year**: Integer 1-4, optional
- **Phone**: 10+ digits, optional
- **Department**: 2-255 chars, optional

### Load
- Uses `ON CONFLICT (email) DO NOTHING` for idempotency
- Automatically creates missing departments
- Invalid records stored in `invalid_rows` table as JSON
- Metrics tracked in `etl_runs` table

## Database Schema

### students
```sql
- id (PRIMARY KEY)
- email (UNIQUE, NOT NULL)
- name (NOT NULL)
- department_id (FOREIGN KEY)
- year (1-4, optional)
- phone (optional)
- created_at
- updated_at
```

### departments
```sql
- id (PRIMARY KEY)
- name (UNIQUE, NOT NULL)
- created_at
```

### etl_runs (metrics)
```sql
- id (PRIMARY KEY)
- run_timestamp
- total_rows
- valid_rows
- invalid_rows
- inserted_rows
- updated_rows
- duration_seconds
- status (success/partial_success/failed)
- error_message
```

### invalid_rows (dead-letter queue)
```sql
- id (PRIMARY KEY)
- etl_run_id (FOREIGN KEY)
- raw_data (JSONB)
- error_reason (TEXT)
- row_number
- created_at
```

## Usage Examples

### Run Pipeline with Custom Configuration

```python
from etl.run_etl import ETLOrchestrator
from config.settings import Settings

settings = Settings()
orchestrator = ETLOrchestrator(settings)
success = orchestrator.run()
```

### Validate Data Before Loading

```python
from etl.validator import StudentRecordValidator

validator = StudentRecordValidator()
records = [
    {"email": "john@example.com", "name": "John Doe", "year": 1},
    {"email": "invalid", "name": "Jane Doe"},  # Invalid email
]

valid, invalid = validator.validate_batch(records)
print(f"Valid: {len(valid)}, Invalid: {len(invalid)}")
```

### Extract from Specific Range

```python
from etl.extract import GoogleSheetsExtractor
from config.settings import Settings

settings = Settings()
extractor = GoogleSheetsExtractor(settings.GOOGLE_CREDENTIALS_PATH)

# Get first 1000 rows
df = extractor.extract_range(
    sheet_id=settings.GOOGLE_SHEET_ID,
    sheet_name="Students",
    cell_range="A1:F1000"
)
```

### Query Invalid Records

```python
from db.connection import DatabaseConnection

# Get invalid records from latest run
query = """
    SELECT ir.raw_data, ir.error_reason, er.run_timestamp
    FROM invalid_rows ir
    JOIN etl_runs er ON ir.etl_run_id = er.id
    WHERE er.status != 'success'
    ORDER BY er.run_timestamp DESC
    LIMIT 100;
"""

results = DatabaseConnection.execute_query(query)
for raw_data, error_reason, timestamp in results:
    print(f"{timestamp}: {error_reason}")
    print(raw_data)
```

## Logging

Logs are written to `logs/etl.log` and console:

```
2025-12-15 14:23:45 | INFO     | etl.run_etl | ============================================================
2025-12-15 14:23:45 | INFO     | etl.run_etl | Starting ETL Pipeline
2025-12-15 14:23:45 | INFO     | etl.run_etl | ============================================================
2025-12-15 14:23:45 | INFO     | db.connection | Database connection established
2025-12-15 14:23:45 | INFO     | etl.run_etl | Created ETL run record: 1
2025-12-15 14:23:46 | INFO     | etl.extract | Successfully extracted 150 rows from Students
2025-12-15 14:23:47 | INFO     | etl.transform | Validation complete: 148 valid, 2 invalid
2025-12-15 14:23:47 | INFO     | etl.load | Successfully loaded students: 145 inserted, 3 skipped
```

## Error Handling

- Invalid rows are isolated in `invalid_rows` table with error reasons
- Connection failures automatically retry (configurable)
- Batch failures don't stop the pipeline
- All errors logged with full context

## Performance

- Connection pooling reduces overhead
- Batch inserts for efficient bulk loading
- Indexes on email and department_id for fast lookups
- JSON storage for flexible invalid row analysis

## Future Enhancements

- [ ] Multi-sheet support
- [ ] Incremental sync (only new rows)
- [ ] API endpoint wrapper
- [ ] Scheduled runs with APScheduler
- [ ] Data profiling and quality reports
- [ ] Webhook notifications on failures
- [ ] Web dashboard for metrics

## Troubleshooting

### Database Connection Fails
- Verify host, port, credentials in `.env`
- Check network connectivity to database
- Ensure database exists and user has permissions

### Google Sheets API Error
- Verify `credentials.json` is valid
- Ensure service account has access to the sheet
- Check API is enabled in Google Cloud Console

### Validation Failures
- Review `invalid_rows` table for error details
- Check data format matches expected types
- Ensure required fields are present

### Slow Performance
- Increase `BATCH_SIZE` in `.env`
- Add indexes on frequently filtered columns
- Monitor database connection pool usage

## License

MIT
#   s h e e t g u a r d  
 