"""
Field-Level Validation Rules

Defines validation rules for each field in the student record.
Provides reusable validators for data quality checks.
"""

import re
import logging
from typing import Any, Tuple, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Validator(ABC):
    """Abstract base class for field validators."""

    @abstractmethod
    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate a value.

        Returns:
            Tuple of (is_valid, error_message)
        """
        pass


class EmailValidator(Validator):
    """Validates email addresses."""

    EMAIL_REGEX = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )

    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate email format.

        Args:
            value: Email address to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not value or not isinstance(value, str):
            return False, "Email is required and must be a string"

        value = value.strip().lower()

        if not self.EMAIL_REGEX.match(value):
            return False, f"Invalid email format: {value}"

        if len(value) > 255:
            return False, "Email exceeds maximum length of 255 characters"

        return True, None


class NameValidator(Validator):
    """Validates student names."""

    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate student name.

        Args:
            value: Name to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not value or not isinstance(value, str):
            return False, "Name is required and must be a string"

        value = value.strip()

        if len(value) < 2:
            return False, "Name must be at least 2 characters"

        if len(value) > 255:
            return False, "Name exceeds maximum length of 255 characters"

        # Allow letters, spaces, hyphens, apostrophes
        if not re.match(r"^[a-zA-Z\s\-']+$", value):
            return False, f"Name contains invalid characters: {value}"

        return True, None


class YearValidator(Validator):
    """Validates student year (1-4)."""

    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate year of study.

        Args:
            value: Year value to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None or value == "":
            # Year is optional
            return True, None

        # Try to convert to integer
        try:
            year = int(str(value).strip())
        except (ValueError, TypeError):
            return False, f"Year must be a valid integer, got: {value}"

        if year < 1 or year > 4:
            return False, f"Year must be between 1 and 4, got: {year}"

        return True, None


class PhoneValidator(Validator):
    """Validates phone numbers."""

    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate phone number format.

        Args:
            value: Phone number to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None or value == "":
            # Phone is optional
            return True, None

        if not isinstance(value, str):
            return False, "Phone must be a string"

        value = value.strip()

        # Allow digits, spaces, hyphens, plus, parentheses
        if not re.match(r"^[\d\s\-+()]+$", value):
            return False, f"Phone contains invalid characters: {value}"

        # Remove non-digit characters and check minimum length
        digits_only = re.sub(r"\D", "", value)
        if len(digits_only) < 10:
            return False, "Phone number must contain at least 10 digits"

        if len(value) > 20:
            return False, "Phone number exceeds maximum length of 20 characters"

        return True, None


class DepartmentValidator(Validator):
    """Validates department names."""

    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate department name.

        Args:
            value: Department name to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if value is None or value == "":
            # Department is optional
            return True, None

        if not isinstance(value, str):
            return False, "Department must be a string"

        value = value.strip()

        if len(value) < 2:
            return False, "Department name must be at least 2 characters"

        if len(value) > 255:
            return False, "Department name exceeds maximum length of 255 characters"

        return True, None


class StudentRecordValidator:
    """
    Validates complete student records using field validators.
    """

    def __init__(self):
        """Initialize validators for each field."""
        self.validators = {
            "email": EmailValidator(),
            "name": NameValidator(),
            "year": YearValidator(),
            "phone": PhoneValidator(),
            "department": DepartmentValidator(),
        }

    def validate_record(self, record: dict) -> Tuple[bool, Optional[str]]:
        """
        Validate a complete student record.

        Args:
            record: Dictionary with student data

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check required fields
        required_fields = ["email", "name"]
        for field in required_fields:
            if field not in record or not record.get(field):
                return False, f"Required field missing: {field}"

        # Validate each field
        for field, validator in self.validators.items():
            if field in record:
                is_valid, error = validator.validate(record[field])
                if not is_valid:
                    return False, f"{field}: {error}"

        return True, None

    def validate_batch(self, records: list) -> Tuple[list, list]:
        """
        Validate a batch of records.

        Args:
            records: List of dictionaries with student data

        Returns:
            Tuple of (valid_records, invalid_records_with_reasons)
        """
        valid_records = []
        invalid_records = []

        for idx, record in enumerate(records, 1):
            is_valid, error = self.validate_record(record)

            if is_valid:
                valid_records.append(record)
            else:
                invalid_records.append({
                    "row_number": idx,
                    "record": record,
                    "error_reason": error,
                })

        logger.info(f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid")

        return valid_records, invalid_records
