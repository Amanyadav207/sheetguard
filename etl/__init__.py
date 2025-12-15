"""
ETL Pipeline Package

A production-grade ETL system for ingesting data from Google Sheets into PostgreSQL.

Modules:
- extract: Data extraction from Google Sheets
- transform: Data cleaning and normalization
- validator: Field-level validation rules
- load: Idempotent database inserts
- run_etl: Pipeline orchestration
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"
