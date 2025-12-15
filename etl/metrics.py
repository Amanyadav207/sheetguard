"""
Data Quality Metrics & Reporting

Provides programmatic access to ETL quality metrics and trends.
Supports dashboards, alerts, and analytical queries.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from db.connection import DatabaseConnection

logger = logging.getLogger(__name__)


@dataclass
class QualityMetrics:
    """Data quality metrics for a single ETL run."""
    run_id: int
    run_timestamp: datetime
    total_rows: int
    valid_rows: int
    invalid_rows: int
    duplicate_emails: int
    inserted_rows: int
    skipped_rows: int
    duration_seconds: float
    status: str
    
    @property
    def validity_rate(self) -> float:
        """Calculate validity rate as percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.valid_rows / self.total_rows) * 100
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate as percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.invalid_rows / self.total_rows) * 100
    
    @property
    def duplicate_rate(self) -> float:
        """Calculate duplicate rate as percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.duplicate_emails / self.total_rows) * 100
    
    @property
    def skip_rate(self) -> float:
        """Calculate skip rate as percentage of processed rows."""
        total_processed = self.inserted_rows + self.skipped_rows
        if total_processed == 0:
            return 0.0
        return (self.skipped_rows / total_processed) * 100
    
    @property
    def throughput(self) -> float:
        """Calculate rows per second."""
        if self.duration_seconds == 0:
            return 0.0
        return self.total_rows / self.duration_seconds


@dataclass
class DailyMetrics:
    """Daily aggregated metrics."""
    run_date: str
    runs_count: int
    total_rows: int
    valid_rows: int
    invalid_rows: int
    duplicates: int
    inserted: int
    skipped: int
    avg_duration: float
    successful_runs: int
    failed_runs: int
    
    @property
    def daily_validity_rate(self) -> float:
        """Daily validity rate as percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.valid_rows / self.total_rows) * 100
    
    @property
    def daily_error_rate(self) -> float:
        """Daily error rate as percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.invalid_rows / self.total_rows) * 100


class QualityMetricsProvider:
    """Provides access to ETL quality metrics and trends."""
    
    @staticmethod
    def get_latest_run() -> Optional[QualityMetrics]:
        """
        Get metrics from the most recent ETL run.
        
        Returns:
            QualityMetrics object or None if no runs
        """
        query = """
            SELECT 
                id, run_timestamp, total_rows, valid_rows, invalid_rows,
                duplicate_emails, inserted_rows, skipped_rows, duration_seconds, status
            FROM etl_runs
            ORDER BY run_timestamp DESC
            LIMIT 1;
        """
        
        try:
            result = DatabaseConnection.execute_query(query)
            if not result:
                return None
            
            row = result[0]
            return QualityMetrics(
                run_id=row[0],
                run_timestamp=row[1],
                total_rows=row[2],
                valid_rows=row[3],
                invalid_rows=row[4],
                duplicate_emails=row[5],
                inserted_rows=row[6],
                skipped_rows=row[7],
                duration_seconds=row[8],
                status=row[9],
            )
        except Exception as e:
            logger.error(f"Failed to fetch latest metrics: {e}")
            return None
    
    @staticmethod
    def get_daily_metrics(days: int = 30) -> List[DailyMetrics]:
        """
        Get daily aggregated metrics.
        
        Args:
            days: Number of days to fetch (default: 30)
        
        Returns:
            List of DailyMetrics objects
        """
        query = """
            SELECT 
                DATE(run_timestamp) AS run_date,
                COUNT(*) AS daily_runs,
                SUM(total_rows) AS total_rows_processed,
                SUM(valid_rows) AS total_valid_rows,
                SUM(invalid_rows) AS total_invalid_rows,
                SUM(duplicate_emails) AS total_duplicates,
                SUM(inserted_rows) AS total_inserted,
                SUM(skipped_rows) AS total_skipped,
                ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs
            FROM etl_runs
            WHERE run_timestamp > NOW() - INTERVAL %s
            GROUP BY DATE(run_timestamp)
            ORDER BY run_date DESC;
        """
        
        try:
            results = DatabaseConnection.execute_query(
                query, (f"{days} days",)
            )
            
            return [
                DailyMetrics(
                    run_date=str(row[0]),
                    runs_count=row[1],
                    total_rows=row[2],
                    valid_rows=row[3],
                    invalid_rows=row[4],
                    duplicates=row[5],
                    inserted=row[6],
                    skipped=row[7],
                    avg_duration=row[8],
                    successful_runs=row[9],
                    failed_runs=row[10],
                )
                for row in results
            ]
        except Exception as e:
            logger.error(f"Failed to fetch daily metrics: {e}")
            return []
    
    @staticmethod
    def get_error_breakdown(limit: int = 15) -> List[Dict[str, Any]]:
        """
        Get top errors by frequency.
        
        Args:
            limit: Number of top errors to return
        
        Returns:
            List of error breakdown dictionaries
        """
        query = """
            SELECT 
                error_reason,
                COUNT(*) AS frequency,
                COUNT(DISTINCT etl_run_id) AS affected_runs,
                ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM invalid_rows), 2) AS pct_of_total,
                MAX(created_at) AS last_occurrence
            FROM invalid_rows
            GROUP BY error_reason
            ORDER BY frequency DESC
            LIMIT %s;
        """
        
        try:
            results = DatabaseConnection.execute_query(query, (limit,))
            
            return [
                {
                    "error_reason": row[0],
                    "frequency": row[1],
                    "affected_runs": row[2],
                    "pct_of_total": row[3],
                    "last_occurrence": row[4],
                }
                for row in results
            ]
        except Exception as e:
            logger.error(f"Failed to fetch error breakdown: {e}")
            return []
    
    @staticmethod
    def get_health_status() -> Dict[str, Any]:
        """
        Get overall ETL health status.
        
        Returns:
            Dictionary with health metrics
        """
        query = """
            SELECT 
                (SELECT COUNT(*) FROM etl_runs) AS total_runs,
                (SELECT COUNT(*) FROM etl_runs WHERE status = 'success') AS successful_runs,
                (SELECT SUM(total_rows) FROM etl_runs) AS all_rows_processed,
                (SELECT SUM(valid_rows) FROM etl_runs) AS all_valid_rows,
                (SELECT SUM(invalid_rows) FROM etl_runs) AS all_invalid_rows,
                (SELECT COUNT(*) FROM students) AS current_students,
                (SELECT MAX(run_timestamp) FROM etl_runs) AS last_run_time;
        """
        
        try:
            result = DatabaseConnection.execute_query(query)[0]
            
            total_rows = result[2] or 0
            valid_rows = result[3] or 0
            overall_validity = (valid_rows / total_rows * 100) if total_rows > 0 else 0
            
            return {
                "total_runs": result[0],
                "successful_runs": result[1],
                "total_rows_processed": result[2],
                "total_valid_rows": result[3],
                "total_invalid_rows": result[4],
                "current_students": result[5],
                "last_run_time": result[6],
                "overall_validity_pct": round(overall_validity, 2),
            }
        except Exception as e:
            logger.error(f"Failed to fetch health status: {e}")
            return {}
    
    @staticmethod
    def get_quality_scorecard() -> List[Dict[str, Any]]:
        """
        Get quality scorecard comparing current vs historical.
        
        Returns:
            List with current, 7-day, and 30-day metrics
        """
        try:
            latest = QualityMetricsProvider.get_latest_run()
            daily_7d = QualityMetricsProvider.get_daily_metrics(days=7)
            daily_30d = QualityMetricsProvider.get_daily_metrics(days=30)
            
            scorecard = []
            
            # Current run
            if latest:
                scorecard.append({
                    "period": "Current (Last Run)",
                    "run_time": latest.run_timestamp,
                    "total_rows": latest.total_rows,
                    "valid_rows": latest.valid_rows,
                    "invalid_rows": latest.invalid_rows,
                    "duplicates": latest.duplicate_emails,
                    "validity_pct": round(latest.validity_rate, 2),
                    "status": latest.status,
                })
            
            # 7-day average
            if daily_7d:
                total_rows_7d = sum(d.total_rows for d in daily_7d)
                valid_rows_7d = sum(d.valid_rows for d in daily_7d)
                invalid_rows_7d = sum(d.invalid_rows for d in daily_7d)
                duplicates_7d = sum(d.duplicates for d in daily_7d)
                
                validity_7d = (valid_rows_7d / total_rows_7d * 100) if total_rows_7d > 0 else 0
                
                scorecard.append({
                    "period": "Last 7 Days Average",
                    "run_time": None,
                    "total_rows": round(total_rows_7d / len(daily_7d), 0),
                    "valid_rows": round(valid_rows_7d / len(daily_7d), 0),
                    "invalid_rows": round(invalid_rows_7d / len(daily_7d), 0),
                    "duplicates": round(duplicates_7d / len(daily_7d), 0),
                    "validity_pct": round(validity_7d, 2),
                    "status": "N/A",
                })
            
            # 30-day average
            if daily_30d:
                total_rows_30d = sum(d.total_rows for d in daily_30d)
                valid_rows_30d = sum(d.valid_rows for d in daily_30d)
                invalid_rows_30d = sum(d.invalid_rows for d in daily_30d)
                duplicates_30d = sum(d.duplicates for d in daily_30d)
                
                validity_30d = (valid_rows_30d / total_rows_30d * 100) if total_rows_30d > 0 else 0
                
                scorecard.append({
                    "period": "Last 30 Days Average",
                    "run_time": None,
                    "total_rows": round(total_rows_30d / len(daily_30d), 0),
                    "valid_rows": round(valid_rows_30d / len(daily_30d), 0),
                    "invalid_rows": round(invalid_rows_30d / len(daily_30d), 0),
                    "duplicates": round(duplicates_30d / len(daily_30d), 0),
                    "validity_pct": round(validity_30d, 2),
                    "status": "N/A",
                })
            
            return scorecard
        except Exception as e:
            logger.error(f"Failed to generate quality scorecard: {e}")
            return []
    
    @staticmethod
    def check_quality_degradation(threshold_pct: float = 90.0) -> Dict[str, Any]:
        """
        Check if data quality has degraded below threshold.
        
        Args:
            threshold_pct: Validity percentage threshold (default: 90%)
        
        Returns:
            Dictionary with degradation status and details
        """
        latest = QualityMetricsProvider.get_latest_run()
        
        if not latest:
            return {"degraded": False, "reason": "No ETL runs found"}
        
        if latest.validity_rate < threshold_pct:
            return {
                "degraded": True,
                "reason": f"Validity rate {latest.validity_rate:.2f}% is below threshold {threshold_pct}%",
                "validity_rate": latest.validity_rate,
                "threshold": threshold_pct,
                "run_id": latest.run_id,
                "run_timestamp": latest.run_timestamp,
            }
        
        return {
            "degraded": False,
            "reason": f"Validity rate {latest.validity_rate:.2f}% is above threshold {threshold_pct}%",
            "validity_rate": latest.validity_rate,
            "threshold": threshold_pct,
        }
