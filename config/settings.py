"""
Configuration Management

Loads environment variables and provides settings for the ETL pipeline.
Uses python-dotenv for local development and environment variables for production.
"""

import os
from typing import Optional
from dotenv import load_dotenv

# Load .env file for local development
load_dotenv()


class Settings:
    """
    Application settings loaded from environment variables.
    
    Ensures no hardcoded credentials in code.
    """

    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "etl_db")
    DB_USER: str = os.getenv("DB_USER")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD")

    # Google Sheets Configuration
    GOOGLE_SHEET_ID: str = os.getenv("GOOGLE_SHEET_ID")
    GOOGLE_CREDENTIALS_PATH: str = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")

    # ETL Configuration
    SHEET_NAME: str = os.getenv("SHEET_NAME", "Students")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/etl.log")

    def __init__(self):
        """Validate required settings on initialization."""
        self._validate_settings()

    def _validate_settings(self) -> None:
        """
        Validate that all required settings are provided.
        
        Raises:
            ValueError: If required settings are missing
        """
        required_fields = ["DB_USER", "DB_PASSWORD", "GOOGLE_SHEET_ID"]
        
        missing_fields = [
            field for field in required_fields
            if not getattr(self, field, None)
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_fields)}. "
                f"Please check your .env file."
            )

    def __repr__(self) -> str:
        """Return string representation (excluding sensitive data)."""
        return (
            f"Settings("
            f"DB_HOST={self.DB_HOST}, "
            f"DB_NAME={self.DB_NAME}, "
            f"SHEET_NAME={self.SHEET_NAME}, "
            f"BATCH_SIZE={self.BATCH_SIZE}"
            f")"
        )
