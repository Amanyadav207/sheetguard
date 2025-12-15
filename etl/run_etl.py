"""
ETL Pipeline Orchestrator

Coordinates the complete ETL workflow:
- Extract data from Google Sheets
- Transform and validate
- Load into PostgreSQL
- Track metrics and errors
"""

import logging
import time
from datetime import datetime, timezone
from typing import Tuple, Dict, Any
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import DatabaseConnection
from config.settings import Settings
from etl.extract import fetch_google_sheet
from etl.transform import validate_and_transform
from etl.load import load_students, load_invalid_rows

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """
    Orchestrates the complete ETL pipeline.

    Workflow:
    1. Initialize database connection
    2. Extract data from Google Sheets
    3. Transform and validate rows
    4. Load valid rows into database
    5. Load invalid rows into dead-letter queue
    6. Record metrics in etl_runs table
    """

    def __init__(self, settings: Settings):
        """
        Initialize ETL orchestrator.

        Args:
            settings: Configuration object with database and API credentials
        """
        self.settings = settings
        self.etl_run_id: int = None
        self.start_time: datetime = None
        self.end_time: datetime = None
        self.metrics: Dict[str, Any] = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "inserted_rows": 0,
            "updated_rows": 0,
        }

    def run(self) -> bool:
        """
        Execute the complete ETL pipeline.

        Returns:
            True if successful, False otherwise
        """
        self.start_time = datetime.now(timezone.utc)
        
        try:
            logger.info("=" * 60)
            logger.info("Starting ETL Pipeline")
            logger.info("=" * 60)

            # Initialize database connection
            self._initialize_database()

            # Initialize ETL run record
            self._create_etl_run_record()

            # Extract, transform, load
            self._execute_pipeline()

            # Update metrics and mark as success
            self._finalize_etl_run("success")

            logger.info("=" * 60)
            logger.info("ETL Pipeline Completed Successfully")
            logger.info("=" * 60)
            self._log_summary()

            return True

        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
            self._finalize_etl_run("failed", str(e))
            return False

        finally:
            DatabaseConnection.close_all()

    def _initialize_database(self) -> None:
        """Initialize database connection pool."""
        logger.info("Initializing database connection...")
        try:
            DatabaseConnection.initialize(
                host=self.settings.DB_HOST,
                port=self.settings.DB_PORT,
                database=self.settings.DB_NAME,
                user=self.settings.DB_USER,
                password=self.settings.DB_PASSWORD,
                min_connections=1,
                max_connections=5,
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    def _create_etl_run_record(self) -> None:
        """Create initial ETL run record."""
        query = """
            INSERT INTO etl_runs (total_rows, valid_rows, invalid_rows, inserted_rows, updated_rows, status)
            VALUES (0, 0, 0, 0, 0, 'in_progress')
            RETURNING id;
        """
        with DatabaseConnection.get_cursor() as cursor:
            cursor.execute(query)
            self.etl_run_id = cursor.fetchone()[0]
        logger.info(f"Created ETL run record: {self.etl_run_id}")

    def _execute_pipeline(self) -> None:
        """
        Execute the main ETL pipeline.
        
        Steps:
        1. Extract data from Google Sheets
        2. Transform and validate records
        3. Load valid records into students table
        4. Load invalid records to dead-letter queue
        """
        logger.info("Executing ETL pipeline steps...")

        # EXTRACT
        logger.info("Step 1: Extracting data from Google Sheets...")
        df = fetch_google_sheet(self.settings)
        self.metrics["total_rows"] = len(df)

        # TRANSFORM & VALIDATE
        logger.info("Step 2: Transforming and validating data...")
        valid_records, invalid_records, transform_metrics = validate_and_transform(df)
        
        # Update metrics from transform
        self.metrics["total_rows"] = transform_metrics["total_rows"]
        self.metrics["valid_rows"] = transform_metrics["valid_rows"]
        self.metrics["invalid_rows"] = transform_metrics["invalid_rows"]
        self.metrics["duplicate_emails"] = transform_metrics.get("duplicate_emails", 0)

        # LOAD VALID RECORDS
        if valid_records:
            logger.info("Step 3: Loading valid records into database...")
            load_metrics = load_students(valid_records, batch_size=self.settings.BATCH_SIZE)
            self.metrics["inserted_rows"] = load_metrics["inserted"]
            self.metrics["skipped_rows"] = load_metrics["skipped"]
        else:
            logger.warning("No valid records to load")
            self.metrics["inserted_rows"] = 0
            self.metrics["skipped_rows"] = 0

        # LOAD INVALID RECORDS TO DEAD-LETTER QUEUE
        if invalid_records:
            logger.info(f"Step 4: Loading {len(invalid_records)} invalid records to dead-letter queue...")
            load_invalid_rows(invalid_records, self.etl_run_id)
        else:
            logger.info("No invalid records to load")

        logger.info("Pipeline execution completed")

    def _finalize_etl_run(self, status: str, error_message: str = None) -> None:
        """
        Update ETL run record with final metrics.

        Args:
            status: Final status ('success', 'partial_success', 'failed')
            error_message: Error details if failed
        """
        self.end_time = datetime.now(timezone.utc)
        duration = (self.end_time - self.start_time).total_seconds()

        query = """
            UPDATE etl_runs
            SET status = %s,
                total_rows = %s,
                valid_rows = %s,
                invalid_rows = %s,
                duplicate_emails = %s,
                inserted_rows = %s,
                skipped_rows = %s,
                updated_rows = %s,
                duration_seconds = %s,
                error_message = %s
            WHERE id = %s;
        """

        params = (
            status,
            self.metrics["total_rows"],
            self.metrics["valid_rows"],
            self.metrics["invalid_rows"],
            self.metrics.get("duplicate_emails", 0),
            self.metrics["inserted_rows"],
            self.metrics.get("skipped_rows", 0),
            self.metrics["updated_rows"],
            duration,
            error_message,
            self.etl_run_id,
        )

        DatabaseConnection.execute_update(query, params)
        logger.info(f"ETL run {self.etl_run_id} finalized with status: {status}")

    def _log_summary(self) -> None:
        """Log ETL execution summary with all metrics."""
        duration = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total rows processed: {self.metrics['total_rows']}")
        logger.info(f"Valid rows: {self.metrics['valid_rows']}")
        logger.info(f"Invalid rows: {self.metrics['invalid_rows']}")
        logger.info(f"Duplicate emails detected: {self.metrics.get('duplicate_emails', 0)}")
        logger.info(f"Inserted rows: {self.metrics['inserted_rows']}")
        logger.info(f"Skipped rows (duplicates): {self.metrics.get('skipped_rows', 0)}")
        logger.info(f"Updated rows: {self.metrics['updated_rows']}")
        
        # Calculate and log quality metrics
        if self.metrics['total_rows'] > 0:
            validity_rate = (self.metrics['valid_rows'] / self.metrics['total_rows']) * 100
            error_rate = (self.metrics['invalid_rows'] / self.metrics['total_rows']) * 100
            duplicate_rate = (self.metrics.get('duplicate_emails', 0) / self.metrics['total_rows']) * 100
            logger.info(f"Data validity rate: {validity_rate:.2f}%")
            logger.info(f"Error rate: {error_rate:.2f}%")
            logger.info(f"Duplicate rate: {duplicate_rate:.2f}%")


def setup_logging(log_file: str = "logs/etl.log") -> None:
    """
    Configure logging for ETL pipeline.

    Args:
        log_file: Path to log file
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


def main() -> None:
    """Main entry point for ETL pipeline."""
    setup_logging()
    
    try:
        settings = Settings()
        orchestrator = ETLOrchestrator(settings)
        success = orchestrator.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
