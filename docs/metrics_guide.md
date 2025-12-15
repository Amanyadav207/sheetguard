# ETL Data Quality Metrics Guide

## Overview

The ETL system automatically computes and stores comprehensive data quality metrics at each run. Metrics are persistent in the database and exposed through SQL views and a Python metrics API.

## Computed Metrics

### Per-Run Metrics (Stored in `etl_runs` table)

| Metric | Description |
|--------|-------------|
| `total_rows` | Total rows extracted from Google Sheets |
| `valid_rows` | Rows that passed validation |
| `invalid_rows` | Rows with validation errors |
| `duplicate_emails` | Duplicate emails detected within batch |
| `inserted_rows` | Rows successfully inserted into database |
| `skipped_rows` | Rows skipped (already exist, ON CONFLICT) |
| `duration_seconds` | Total ETL execution time |
| `status` | 'success', 'partial_success', or 'failed' |

### Calculated Metrics

| Metric | Formula | Use Case |
|--------|---------|----------|
| `validity_rate` | (valid_rows / total_rows) * 100 | Overall data quality percentage |
| `error_rate` | (invalid_rows / total_rows) * 100 | Percentage of bad records |
| `duplicate_rate` | (duplicate_emails / total_rows) * 100 | Data deduplication rate |
| `skip_rate` | (skipped_rows / (inserted + skipped)) * 100 | Percentage of already-loaded records |
| `throughput` | total_rows / duration_seconds | Rows processed per second |

## Database Views

### `vw_etl_quality_metrics`
Latest ETL runs with all quality metrics and calculated percentages.

```sql
SELECT * FROM vw_etl_quality_metrics LIMIT 10;
```

**Columns**:
- run_id, run_timestamp, total_rows, valid_rows, invalid_rows, duplicate_emails
- inserted_rows, skipped_rows, duration_seconds, status
- validity_rate_pct, error_rate_pct, duplicate_rate_pct, skip_rate_pct

### `vw_daily_quality_summary`
Daily aggregated metrics for trend analysis.

```sql
SELECT * FROM vw_daily_quality_summary LIMIT 30;
```

**Columns**:
- run_date, total_runs
- total_rows_processed, total_valid_rows, total_invalid_rows, total_duplicates
- total_inserted, total_skipped
- avg_duration_sec, successful_runs, failed_runs
- daily_validity_rate_pct, daily_error_rate_pct

### `vw_error_breakdown`
Top error reasons ranked by frequency.

```sql
SELECT * FROM vw_error_breakdown LIMIT 15;
```

**Columns**:
- error_reason, frequency, affected_runs, pct_of_total
- last_occurrence, first_occurrence

### `vw_quality_trends_7day`
7-day rolling window of quality metrics.

```sql
SELECT * FROM vw_quality_trends_7day;
```

### `vw_performance_metrics`
Throughput analysis (rows/second).

```sql
SELECT * FROM vw_performance_metrics LIMIT 20;
```

### `vw_quality_scorecard`
Comparison of current metrics vs historical averages.

```sql
SELECT * FROM vw_quality_scorecard;
```

## Python Metrics API

### Basic Usage

```python
from etl.metrics import QualityMetricsProvider

# Get latest run metrics
latest = QualityMetricsProvider.get_latest_run()
print(f"Validity: {latest.validity_rate:.2f}%")
print(f"Errors: {latest.error_rate:.2f}%")
print(f"Throughput: {latest.throughput:.2f} rows/sec")
```

### QualityMetricsProvider Methods

#### `get_latest_run() → QualityMetrics`
Returns the most recent ETL run metrics.

```python
latest = QualityMetricsProvider.get_latest_run()
if latest:
    print(f"Run {latest.run_id}: {latest.validity_rate:.2f}% valid")
    print(f"Status: {latest.status}")
```

#### `get_daily_metrics(days: int = 30) → List[DailyMetrics]`
Returns daily aggregated metrics.

```python
daily = QualityMetricsProvider.get_daily_metrics(days=7)
for day in daily:
    print(f"{day.run_date}: {day.daily_validity_rate:.2f}% valid, {day.runs_count} runs")
```

#### `get_error_breakdown(limit: int = 15) → List[Dict]`
Returns top error reasons.

```python
errors = QualityMetricsProvider.get_error_breakdown(limit=10)
for error in errors:
    print(f"{error['error_reason']}: {error['frequency']} occurrences")
```

#### `get_health_status() → Dict`
Returns overall ETL health snapshot.

```python
health = QualityMetricsProvider.get_health_status()
print(f"Total students: {health['current_students']}")
print(f"Overall validity: {health['overall_validity_pct']:.2f}%")
print(f"Last run: {health['last_run_time']}")
```

#### `get_quality_scorecard() → List[Dict]`
Compares current vs historical metrics.

```python
scorecard = QualityMetricsProvider.get_quality_scorecard()
for row in scorecard:
    print(f"{row['period']}: {row['validity_pct']}%")
```

#### `check_quality_degradation(threshold_pct: float = 90.0) → Dict`
Checks if quality has degraded below threshold.

```python
check = QualityMetricsProvider.check_quality_degradation(threshold_pct=90)
if check['degraded']:
    print(f"ALERT: {check['reason']}")
else:
    print("Quality is healthy")
```

## Example Queries

### Get Latest Run Summary
```sql
SELECT 
    id, 
    run_timestamp,
    total_rows,
    valid_rows,
    invalid_rows,
    duplicate_emails,
    ROUND(100.0 * valid_rows / total_rows, 2) AS validity_pct,
    duration_seconds,
    status
FROM etl_runs
ORDER BY run_timestamp DESC
LIMIT 1;
```

### Daily Quality Trend (Last 7 Days)
```sql
SELECT 
    DATE(run_timestamp) AS run_date,
    COUNT(*) AS runs,
    ROUND(100.0 * SUM(valid_rows) / SUM(total_rows), 2) AS daily_validity_pct,
    SUM(invalid_rows) AS total_errors,
    SUM(duplicate_emails) AS total_duplicates
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days'
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;
```

### Error Analysis
```sql
SELECT 
    error_reason,
    COUNT(*) AS frequency,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM invalid_rows), 2) AS pct_of_total,
    MAX(created_at) AS last_seen
FROM invalid_rows
GROUP BY error_reason
ORDER BY frequency DESC
LIMIT 10;
```

### Quality Comparison
```sql
SELECT 
    'Current' AS period,
    (SELECT ROUND(100.0 * valid_rows / total_rows, 2) FROM etl_runs ORDER BY id DESC LIMIT 1) AS validity_pct
UNION ALL
SELECT 
    'Last 7 Days Avg',
    ROUND(100.0 * SUM(valid_rows) / SUM(total_rows), 2)
FROM etl_runs
WHERE run_timestamp > NOW() - INTERVAL '7 days';
```

### Throughput Analysis
```sql
SELECT 
    run_timestamp,
    total_rows,
    duration_seconds,
    ROUND(total_rows::float / duration_seconds, 2) AS rows_per_second,
    status
FROM etl_runs
WHERE duration_seconds IS NOT NULL
ORDER BY run_timestamp DESC
LIMIT 10;
```

## Integration Examples

### In Dashboard
```python
# Get metrics for dashboard display
health = QualityMetricsProvider.get_health_status()
scorecard = QualityMetricsProvider.get_quality_scorecard()
degradation = QualityMetricsProvider.check_quality_degradation(threshold_pct=90)

dashboard_data = {
    "health": health,
    "scorecard": scorecard,
    "alerts": [degradation] if degradation['degraded'] else [],
}
```

### In Alerts
```python
# Check quality after each run
latest = QualityMetricsProvider.get_latest_run()

if latest.error_rate > 20:
    send_alert(f"High error rate: {latest.error_rate:.2f}%")

if latest.validity_rate < 80:
    send_alert(f"Critical: Validity at {latest.validity_rate:.2f}%")
```

### In Reports
```python
# Generate weekly quality report
weekly = QualityMetricsProvider.get_daily_metrics(days=7)

for day in weekly:
    print(f"\n{day.run_date}")
    print(f"  Runs: {day.runs_count}")
    print(f"  Rows processed: {day.total_rows}")
    print(f"  Validity: {day.daily_validity_rate:.2f}%")
    print(f"  Errors: {day.invalid_rows}")
    print(f"  Duplicates: {day.duplicates}")
```

## Metrics Storage

### `etl_runs` Table
```
id              SERIAL PRIMARY KEY
run_timestamp   TIMESTAMP
total_rows      INTEGER
valid_rows      INTEGER
invalid_rows    INTEGER
duplicate_emails INTEGER          -- NEW
inserted_rows   INTEGER
updated_rows    INTEGER
skipped_rows    INTEGER            -- NEW
duration_seconds DECIMAL(10,2)
status          VARCHAR(50)
error_message   TEXT
created_at      TIMESTAMP
```

### `invalid_rows` Table
```
id              SERIAL PRIMARY KEY
etl_run_id      INTEGER (FK to etl_runs)
raw_data        JSONB
error_reason    TEXT
row_number      INTEGER
created_at      TIMESTAMP
```

## Performance Considerations

### Indexing
- `etl_runs` has index on `run_timestamp` for fast queries
- `invalid_rows` has index on `etl_run_id` for joins

### View Performance
- Views query materialized data (no aggregation overhead)
- Daily summaries group by date efficiently
- Error breakdown uses single table scan

### Query Tips
```sql
-- Fast: Use indexed columns
SELECT * FROM vw_etl_quality_metrics WHERE run_timestamp > NOW() - INTERVAL '7 days';

-- Slow: Function calls on indexed columns
SELECT * FROM etl_runs WHERE DATE(run_timestamp) > '2025-12-08';

-- Better: Range queries
SELECT * FROM etl_runs WHERE run_timestamp > NOW() - INTERVAL '7 days';
```

## Monitoring Best Practices

### Key Thresholds
- **Validity Rate**: Should be > 95% (Alert if < 90%)
- **Error Rate**: Should be < 5% (Critical if > 10%)
- **Duplicate Rate**: Informational (trend over time)
- **Skip Rate**: Low is good (means less reprocessing)
- **Throughput**: Monitor for performance degradation

### Daily Checklist
1. Check latest run status
2. Review validity rate trend
3. Check error breakdown
4. Monitor duplicate rates
5. Verify throughput performance

### Weekly Report
```python
# Generate weekly summary
daily_7d = QualityMetricsProvider.get_daily_metrics(days=7)
scorecard = QualityMetricsProvider.get_quality_scorecard()
errors = QualityMetricsProvider.get_error_breakdown(limit=5)

print("WEEKLY ETL QUALITY REPORT")
print(f"Average Validity: {scorecard[1]['validity_pct']}%")
print(f"Top Errors:")
for e in errors:
    print(f"  - {e['error_reason']}: {e['frequency']} occurrences")
```

## Troubleshooting

### "No metrics data"
- Check if any ETL runs have completed
- Verify database connection
- Query `etl_runs` directly

### "Metrics not updating"
- Verify `_finalize_etl_run()` is called
- Check for database transaction issues
- Review logs for errors

### "Unexpected metric values"
- Check if records are being deduplicated correctly
- Verify validator is catching all errors
- Review transform metrics calculation

## Future Enhancements

- [ ] Real-time metric streaming
- [ ] Anomaly detection for automatic alerts
- [ ] Predictive quality scoring
- [ ] Custom metric definitions
- [ ] Export metrics to external systems (Datadog, New Relic, etc.)
