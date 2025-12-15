# Data Quality Framework

## Validation Rules

### Email Field
**Required**: Yes  
**Type**: String  
**Rules**:
- Valid email format (RFC 5322 simplified)
- Maximum 255 characters
- Case-normalized to lowercase
- Whitespace trimmed

**Validation Code**:
```python
EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
```

**Examples**:
```
VALID:     user@example.com, john.doe@company.co.uk
INVALID:   invalid, user@, @example.com, user@.com
```

### Name Field
**Required**: Yes  
**Type**: String  
**Rules**:
- 2-255 characters
- Letters, spaces, hyphens, apostrophes only
- Whitespace trimmed

**Valid Characters**: `[a-zA-Z\s\-']`

**Examples**:
```
VALID:     John Doe, Mary-Jane, O'Brien, José (if accent acceptable)
INVALID:   J, John123, John@Doe, John|Doe
```

**Note**: For international names with diacritics, consider using `[a-zA-Z\s\-'äöüæøåñçß]` or Unicode categories.

### Year Field
**Required**: No (Optional)  
**Type**: Integer  
**Rules**:
- 1, 2, 3, or 4 only
- Converts from string/float to integer
- Null/empty treated as None

**Validation**:
```python
if year:
    try:
        year = int(float(str(value).strip()))
        assert 1 <= year <= 4
    except (ValueError, AssertionError):
        raise ValidationError(...)
```

**Examples**:
```
VALID:     1, 2, 3, 4, "2", "2.0", None, ""
INVALID:   0, 5, "abc", -1, 1.5, "4.9"
```

### Phone Field
**Required**: No (Optional)  
**Type**: String  
**Rules**:
- Minimum 10 digits
- Maximum 20 characters
- Allows digits, spaces, hyphens, plus, parentheses
- Whitespace trimmed

**Valid Format**: `[0-9\s\-+()]{10,20}`

**Examples**:
```
VALID:     +1-555-123-4567, (555) 123-4567, 555.123.4567
INVALID:   123, 555-1234, abc-1234
```

### Department Field
**Required**: No (Optional)  
**Type**: String  
**Rules**:
- 2-255 characters
- Alphanumeric and spaces
- Whitespace trimmed
- Creates department record if missing

**Examples**:
```
VALID:     Engineering, Computer Science, Business Administration
INVALID:   CS, D, "", null
```

## Data Quality Checks

### Row-Level Checks
1. Required fields present: `email`, `name`
2. No duplicate emails in batch (first occurrence kept)
3. Valid data types after conversion
4. All field values within constraints

### Dataset-Level Checks
1. Row count within expected range
2. Email uniqueness enforced via UNIQUE constraint
3. Invalid/valid ratio within acceptable bounds
4. No systematic patterns in validation errors

### Sample Quality Metrics
```sql
-- Email validation pass rate
SELECT 
  COUNT(*) total_records,
  COUNT(CASE WHEN email LIKE '%@%.%' THEN 1 END) valid_emails,
  ROUND(100.0 * COUNT(CASE WHEN email LIKE '%@%.%' THEN 1 END) / COUNT(*), 2) pass_rate
FROM invalid_rows
WHERE etl_run_id = ?;

-- Most common validation errors
SELECT 
  error_reason,
  COUNT(*) frequency
FROM invalid_rows
GROUP BY error_reason
ORDER BY frequency DESC
LIMIT 10;

-- Invalid record distribution
SELECT 
  DATE(created_at) run_date,
  COUNT(*) invalid_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (ORDER BY DATE(created_at)), 2) pct
FROM invalid_rows
GROUP BY DATE(created_at)
ORDER BY run_date DESC;
```

## Transformation Rules

### Email
```python
email = email.strip().lower()
```
**Before**: `"  USER@EXAMPLE.COM  "`  
**After**: `"user@example.com"`

### Name
```python
name = name.strip()
```
**Before**: `"  John Doe  "`  
**After**: `"John Doe"`

### Year
```python
year = int(float(str(year).strip())) if year else None
```
**Before**: `"2.0"`, `"2"`, `None`  
**After**: `2`, `2`, `None`

### Phone
```python
phone = phone.strip() if phone else None
```
**Before**: `"  555-123-4567  "`  
**After**: `"555-123-4567"`

### Department
```python
department = department.strip() if department else None
```
**Before**: `"  Engineering  "`  
**After**: `"Engineering"`

## Deduplication

### Strategy: Email-Based
```python
seen_emails = set()
deduplicated = []

for record in records:
    email = record.get("email")
    if email not in seen_emails:
        seen_emails.add(email)
        deduplicated.append(record)
```

**Behavior**: Keeps first occurrence, removes subsequent duplicates.

**Log Example**:
```
WARNING: Removed 3 duplicate records by email
```

## Dead-Letter Queue Analysis

### Query Invalid Records
```sql
-- All invalid records from last run
SELECT * FROM invalid_rows 
WHERE etl_run_id = (SELECT MAX(id) FROM etl_runs)
ORDER BY row_number;

-- Invalid records by error type
SELECT 
  error_reason,
  COUNT(*) count,
  MIN(raw_data) example
FROM invalid_rows
WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY error_reason
ORDER BY count DESC;

-- Records with specific invalid field
SELECT 
  raw_data->>'email' email,
  error_reason,
  created_at
FROM invalid_rows
WHERE raw_data->>'email' IS NOT NULL
  AND raw_data->>'email' NOT LIKE '%@%.%';
```

### Reprocessing Invalid Records
```python
import json
from db.connection import DatabaseConnection
from etl.validator import StudentRecordValidator

# Fetch invalid records
query = """
  SELECT id, raw_data FROM invalid_rows 
  WHERE created_at > NOW() - INTERVAL '1 day'
"""

validator = StudentRecordValidator()

with DatabaseConnection.get_cursor() as cursor:
    cursor.execute(query)
    for record_id, raw_json in cursor.fetchall():
        record = json.loads(raw_json)
        
        # Manually fix and re-validate
        record['email'] = record['email'].lower()
        is_valid, error = validator.validate_record(record)
        
        if is_valid:
            # Attempt to load again
            load_students([record])
            
            # Mark as reprocessed
            delete_query = "DELETE FROM invalid_rows WHERE id = %s"
            DatabaseConnection.execute_update(delete_query, (record_id,))
```

## Quality Metrics Dashboard

### Key Metrics
```python
class QualityMetrics:
    """Track data quality metrics over time."""
    
    def __init__(self, etl_run_id):
        self.etl_run_id = etl_run_id
    
    def get_summary(self):
        query = """
        SELECT 
          total_rows,
          valid_rows,
          invalid_rows,
          inserted_rows,
          ROUND(100.0 * valid_rows / total_rows, 2) validity_rate,
          ROUND(100.0 * inserted_rows / total_rows, 2) insert_rate,
          duration_seconds
        FROM etl_runs
        WHERE id = %s
        """
        return DatabaseConnection.execute_query(query, (self.etl_run_id,))[0]
    
    def get_error_breakdown(self):
        query = """
        SELECT 
          error_reason,
          COUNT(*) count,
          ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM invalid_rows WHERE etl_run_id = %s), 2) percentage
        FROM invalid_rows
        WHERE etl_run_id = %s
        GROUP BY error_reason
        ORDER BY count DESC
        """
        return DatabaseConnection.execute_query(query, (self.etl_run_id, self.etl_run_id))
```

### Reporting
```sql
-- Quality report for last 7 days
SELECT 
  DATE(run_timestamp) run_date,
  COUNT(*) runs,
  ROUND(AVG(valid_rows::float / NULLIF(total_rows, 0)) * 100, 2) avg_validity_rate,
  ROUND(AVG(duration_seconds), 2) avg_duration_sec,
  SUM(invalid_rows) total_invalid_records
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days'
  AND status IN ('success', 'partial_success')
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;
```

## Best Practices

1. **Validate Early**: Check data at extraction, not just loading
2. **Preserve Details**: Store raw data for analysis
3. **Log Everything**: Include row number, field, error message
4. **Separate Concerns**: Don't mix validation with transformation
5. **Reusable Validators**: Write validators once, use everywhere
6. **Type Safety**: Use type hints for IDE support
7. **Idempotency**: Allow safe reruns without side effects
8. **Monitoring**: Track quality metrics over time
9. **Alerts**: Notify on quality degradation
10. **Documentation**: Document all validation rules

## Examples

### Strict Validation
```python
validator = StudentRecordValidator()
is_valid, error = validator.validate_record({
    "email": "john@example.com",
    "name": "John Doe",
    "year": "1"
})
# Result: (True, None)

is_valid, error = validator.validate_record({
    "email": "invalid-email",
    "name": "J"
})
# Result: (False, "email: Invalid email format: invalid-email")
```

### Lenient Transformation
```python
transformer = DataTransformer()
df = pd.DataFrame({
    "Email": ["  USER@EXAMPLE.COM  "],
    "Name": ["  John Doe  "],
    "Year": ["2.0"]
})

valid, invalid = transformer.transform(df)
print(valid[0])
# Output: {
#   "email": "user@example.com",
#   "name": "John Doe",
#   "year": 2
# }
```
