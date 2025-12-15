"""
Google Sheets Data Extraction

Fetches data from Google Sheets and converts to pandas DataFrame.
Handles authentication via service account or OAuth.
"""

import logging
from typing import Optional, List
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class GoogleSheetsExtractor:
    """
    Extracts data from Google Sheets.
    
    Supports both service account and OAuth authentication.
    """

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    def __init__(self, credentials_path: str):
        """
        Initialize Google Sheets extractor.

        Args:
            credentials_path: Path to service account JSON file

        Raises:
            FileNotFoundError: If credentials file not found
            Exception: If authentication fails
        """
        self.credentials_path = credentials_path
        self.client: Optional[gspread.Client] = None
        self._authenticate()

    def _authenticate(self) -> None:
        """
        Authenticate with Google Sheets API using service account.

        Raises:
            FileNotFoundError: If credentials file not found
            Exception: If authentication fails
        """
        try:
            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=self.SCOPES
            )
            self.client = gspread.authorize(credentials)
            logger.info("Successfully authenticated with Google Sheets API")
        except FileNotFoundError:
            logger.error(f"Credentials file not found: {self.credentials_path}")
            raise
        except Exception as e:
            logger.error(f"Google Sheets authentication failed: {e}")
            raise

    def extract(
        self,
        sheet_id: str,
        sheet_name: str = "Sheet1",
        skip_rows: int = 0,
    ) -> pd.DataFrame:
        """
        Extract data from Google Sheet and convert to DataFrame.

        Args:
            sheet_id: Google Sheet ID
            sheet_name: Name of the sheet tab (default: "Sheet1")
            skip_rows: Number of header rows to skip (default: 0)

        Returns:
            pandas DataFrame with extracted data

        Raises:
            Exception: If sheet access fails
        """
        try:
            logger.info(f"Extracting data from sheet: {sheet_name}")

            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)

            # Get all values (including headers)
            data = worksheet.get_all_values()

            if not data:
                logger.warning(f"No data found in sheet {sheet_name}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data[skip_rows:], columns=data[0])

            # Remove empty rows
            df = df.dropna(how="all")

            logger.info(f"Successfully extracted {len(df)} rows from {sheet_name}")
            logger.debug(f"Columns: {list(df.columns)}")

            return df

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet not found: {sheet_id}")
            raise
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet '{sheet_name}' not found in spreadsheet")
            raise
        except Exception as e:
            logger.error(f"Failed to extract data from Google Sheets: {e}")
            raise

    def extract_range(
        self,
        sheet_id: str,
        sheet_name: str,
        cell_range: str,
    ) -> pd.DataFrame:
        """
        Extract data from a specific range in Google Sheet.

        Args:
            sheet_id: Google Sheet ID
            sheet_name: Name of the sheet tab
            cell_range: A1 notation range (e.g., "A1:D100")

        Returns:
            pandas DataFrame with extracted data

        Example:
            df = extractor.extract_range(sheet_id, "Students", "A1:E1000")
        """
        try:
            logger.info(f"Extracting range {cell_range} from {sheet_name}")

            spreadsheet = self.client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)

            # Get specific range
            data = worksheet.batch_get([cell_range])[0]

            if not data:
                logger.warning(f"No data found in range {cell_range}")
                return pd.DataFrame()

            # First row as headers
            df = pd.DataFrame(data[1:], columns=data[0])
            df = df.dropna(how="all")

            logger.info(f"Successfully extracted {len(df)} rows from range {cell_range}")

            return df

        except Exception as e:
            logger.error(f"Failed to extract range {cell_range}: {e}")
            raise


def fetch_google_sheet(settings) -> pd.DataFrame:
    """
    Convenience function to extract data using settings.

    Args:
        settings: Settings object with GOOGLE_CREDENTIALS_PATH and GOOGLE_SHEET_ID

    Returns:
        pandas DataFrame with extracted data
    """
    extractor = GoogleSheetsExtractor(settings.GOOGLE_CREDENTIALS_PATH)
    df = extractor.extract(
        sheet_id=settings.GOOGLE_SHEET_ID,
        sheet_name=settings.SHEET_NAME,
    )
    return df
