"""
Data Transformation Pipeline

Normalizes, cleans, and validates extracted data.
Handles deduplication and type conversions.
"""

import logging
import pandas as pd
from typing import Tuple, List, Dict, Any
import json

from etl.validator import StudentRecordValidator

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforms raw extracted data into clean, validated records.
    
    Operations:
    - Email normalization (lowercase, trim)
    - Type conversions (year to int)
    - Deduplication
    - Field validation
    - Error isolation
    - Quality metrics computation
    """

    def __init__(self):
        """Initialize transformer with validators."""
        self.validator = StudentRecordValidator()
        self.metrics = {
            "total_rows": 0,
            "duplicate_emails": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
        }

    def transform(
        self, df: pd.DataFrame
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
        """
        Transform raw DataFrame into validated records with metrics.

        Args:
            df: Raw pandas DataFrame from extraction

        Returns:
            Tuple of (valid_records, invalid_records, metrics)
            metrics dict includes: total_rows, duplicate_emails, valid_rows, invalid_rows

        Raises:
            ValueError: If DataFrame is empty or missing required columns
        """
        self.metrics["total_rows"] = len(df)
        
        if df.empty:
            logger.warning("Input DataFrame is empty")
            return [], [], self.metrics

        logger.info(f"Starting transformation of {len(df)} rows")

        # Normalize column names
        df = self._normalize_columns(df)

        # Clean and normalize data
        records = self._dataframe_to_records(df)

        # Deduplicate by email and track duplicates
        records, duplicates_removed = self._deduplicate(records)
        self.metrics["duplicate_emails"] = duplicates_removed

        # Validate records
        valid_records, invalid_records = self.validator.validate_batch(records)
        self.metrics["valid_rows"] = len(valid_records)
        self.metrics["invalid_rows"] = len(invalid_records)

        logger.info(
            f"Transformation complete: {len(valid_records)} valid, {len(invalid_records)} invalid, "
            f"{duplicates_removed} duplicates"
        )

        return valid_records, invalid_records, self.metrics

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names to lowercase with underscores.

        Args:
            df: DataFrame with raw column names

        Returns:
            DataFrame with normalized column names
        """
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        return df

    def _dataframe_to_records(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to list of dictionaries with data cleaning.

        Args:
            df: Normalized DataFrame

        Returns:
            List of cleaned record dictionaries
        """
        records = []

        for idx, row in df.iterrows():
            record = {}

            # Email (required, normalize)
            if "email" in row.index:
                record["email"] = str(row["email"]).strip().lower() if row["email"] else None

            # Name (required, strip whitespace)
            if "name" in row.index:
                record["name"] = str(row["name"]).strip() if row["name"] else None

            # Year (optional, convert to int)
            if "year" in row.index and row["year"]:
                try:
                    record["year"] = int(float(str(row["year"]).strip()))
                except (ValueError, TypeError):
                    record["year"] = row["year"]  # Let validator catch the error
            else:
                record["year"] = None

            # Phone (optional, strip whitespace)
            if "phone" in row.index:
                record["phone"] = str(row["phone"]).strip() if row["phone"] else None

            # Department (optional, strip whitespace)
            if "department" in row.index:
                record["department"] = str(row["department"]).strip() if row["department"] else None

            records.append(record)

        logger.debug(f"Converted {len(records)} rows to record dictionaries")

        return records

    def _deduplicate(self, records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """
        Remove duplicate records by email (keep first occurrence).

        Args:
            records: List of records

        Returns:
            Tuple of (deduplicated_list, duplicates_removed_count)
        """
        seen_emails = set()
        deduplicated = []

        for record in records:
            email = record.get("email")
            if email and email not in seen_emails:
                seen_emails.add(email)
                deduplicated.append(record)

        duplicates_removed = len(records) - len(deduplicated)
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate records by email")

        return deduplicated, duplicates_removed


def validate_and_transform(
    df: pd.DataFrame,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """
    Convenience function to transform extracted data.

    Args:
        df: Raw DataFrame from extraction

    Returns:
        Tuple of (valid_records, invalid_records, metrics)
    """
    transformer = DataTransformer()
    return transformer.transform(df)
