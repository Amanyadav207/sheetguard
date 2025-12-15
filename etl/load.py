"""
Data Loading into PostgreSQL

Handles idempotent inserts, upserts, and dead-letter queue management.
Ensures data consistency and tracks load metrics.
"""

import logging
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime

from db.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class StudentLoader:
    """
    Loads student records into PostgreSQL with idempotent behavior.
    
    Uses ON CONFLICT DO NOTHING for idempotent inserts.
    Automatically creates departments as needed.
    """

    def load_students(
        self,
        records: List[Dict[str, Any]],
        batch_size: int = 100,
    ) -> Dict[str, int]:
        """
        Load valid student records into database.

        Uses ON CONFLICT DO NOTHING for idempotency:
        - If email already exists, skip (no error)
        - Suitable for repeated ETL runs with same data

        Args:
            records: List of valid student records
            batch_size: Number of records per batch insert

        Returns:
            Dictionary with metrics: {inserted: int, skipped: int, duplicates: int}
        """
        if not records:
            logger.info("No records to load")
            return {"inserted": 0, "skipped": 0, "duplicates": 0}

        logger.info(f"Loading {len(records)} student records")

        metrics = {"inserted": 0, "skipped": 0, "duplicates": 0}

        try:
            # Ensure departments exist
            self._ensure_departments(records)

            # Load students in batches
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                batch_metrics = self._load_batch(batch)
                metrics["inserted"] += batch_metrics["inserted"]
                metrics["skipped"] += batch_metrics["skipped"]

            logger.info(
                f"Successfully loaded students: "
                f"{metrics['inserted']} inserted, {metrics['skipped']} skipped"
            )

            return metrics

        except Exception as e:
            logger.error(f"Failed to load students: {e}")
            raise

    def _ensure_departments(self, records: List[Dict[str, Any]]) -> None:
        """
        Create departments if they don't exist.

        Args:
            records: List of student records
        """
        departments = set()
        for record in records:
            if record.get("department"):
                departments.add(record["department"])

        if not departments:
            logger.debug("No departments to create")
            return

        logger.info(f"Ensuring {len(departments)} departments exist")

        # Insert departments, ignore if already exist
        query = """
            INSERT INTO departments (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING;
        """

        department_list = [(dept,) for dept in departments]

        try:
            DatabaseConnection.execute_many(query, department_list)
            logger.debug(f"Departments ensured: {', '.join(departments)}")
        except Exception as e:
            logger.error(f"Failed to ensure departments: {e}")
            raise

    def _load_batch(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Load a batch of student records.

        Args:
            records: Batch of student records

        Returns:
            Dictionary with metrics: {inserted: int, skipped: int}
        """
        # Get department IDs
        department_ids = self._get_department_ids(records)

        # Prepare INSERT statement with ON CONFLICT
        query = """
            INSERT INTO students (email, name, department_id, year, phone)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
            RETURNING id;
        """

        data = []
        for record in records:
            dept_id = department_ids.get(record.get("department"))
            data.append(
                (
                    record.get("email"),
                    record.get("name"),
                    dept_id,
                    record.get("year"),
                    record.get("phone"),
                )
            )

        try:
            with DatabaseConnection.get_cursor() as cursor:
                cursor.executemany(query, data)
                inserted = cursor.rowcount

            skipped = len(records) - inserted
            logger.debug(f"Batch loaded: {inserted} inserted, {skipped} skipped")

            return {"inserted": inserted, "skipped": skipped}

        except Exception as e:
            logger.error(f"Failed to load batch: {e}")
            raise

    def _get_department_ids(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Get department IDs for all departments in records.

        Args:
            records: List of student records

        Returns:
            Dictionary mapping department name to ID
        """
        department_names = set()
        for record in records:
            if record.get("department"):
                department_names.add(record["department"])

        if not department_names:
            return {}

        query = "SELECT id, name FROM departments WHERE name = ANY(%s);"
        
        with DatabaseConnection.get_cursor(commit=False) as cursor:
            cursor.execute(query, (list(department_names),))
            results = cursor.fetchall()

        return {name: dept_id for dept_id, name in results}


class InvalidRowLoader:
    """
    Loads invalid/malformed rows into dead-letter queue.
    Preserves raw data as JSON for inspection and reprocessing.
    """

    def load_invalid_rows(
        self,
        invalid_records: List[Dict[str, Any]],
        etl_run_id: int,
    ) -> int:
        """
        Load invalid records into dead-letter queue.

        Args:
            invalid_records: List of invalid records with error reasons
            etl_run_id: Reference to ETL run

        Returns:
            Number of invalid rows inserted
        """
        if not invalid_records:
            logger.info("No invalid records to load")
            return 0

        logger.info(f"Loading {len(invalid_records)} invalid records to dead-letter queue")

        query = """
            INSERT INTO invalid_rows (etl_run_id, raw_data, error_reason, row_number)
            VALUES (%s, %s, %s, %s);
        """

        data = [
            (
                etl_run_id,
                json.dumps(record.get("record", {})),
                record.get("error_reason", "Unknown error"),
                record.get("row_number"),
            )
            for record in invalid_records
        ]

        try:
            inserted = DatabaseConnection.execute_many(query, data)
            logger.info(f"Loaded {inserted} invalid records to dead-letter queue")
            return inserted

        except Exception as e:
            logger.error(f"Failed to load invalid records: {e}")
            raise


def load_students(
    records: List[Dict[str, Any]],
    batch_size: int = 100,
) -> Dict[str, int]:
    """
    Convenience function to load students.

    Args:
        records: List of valid student records
        batch_size: Batch size for inserts

    Returns:
        Dictionary with load metrics
    """
    loader = StudentLoader()
    return loader.load_students(records, batch_size)


def load_invalid_rows(
    invalid_records: List[Dict[str, Any]],
    etl_run_id: int,
) -> int:
    """
    Convenience function to load invalid records.

    Args:
        invalid_records: List of invalid records with errors
        etl_run_id: Reference to ETL run

    Returns:
        Number of invalid rows inserted
    """
    loader = InvalidRowLoader()
    return loader.load_invalid_rows(invalid_records, etl_run_id)
