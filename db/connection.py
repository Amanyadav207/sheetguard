"""
PostgreSQL Connection Helper

Provides connection pooling and context management for database operations.
Handles connection lifecycle and error handling.
"""

import psycopg2
from psycopg2 import pool, OperationalError
from contextlib import contextmanager
from typing import Optional, Generator, Any
import logging

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    Manages PostgreSQL connections with connection pooling.
    """

    _instance = None
    _pool: Optional[pool.SimpleConnectionPool] = None

    def __new__(cls):
        """Ensure singleton pattern for connection pool."""
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    @classmethod
    def initialize(
        cls,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_connections: int = 1,
        max_connections: int = 10,
    ) -> None:
        """
        Initialize the connection pool.

        Args:
            host: PostgreSQL server host
            port: PostgreSQL server port
            database: Database name
            user: Database user
            password: Database password
            min_connections: Minimum pool connections
            max_connections: Maximum pool connections

        Raises:
            OperationalError: If connection fails
        """
        try:
            cls._pool = pool.SimpleConnectionPool(
                min_connections,
                max_connections,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=10,
            )
            logger.info(f"Database pool initialized with {min_connections}-{max_connections} connections")
        except OperationalError as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    @classmethod
    def close_all(cls) -> None:
        """Close all connections in the pool."""
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            logger.info("Database pool closed")

    @classmethod
    @contextmanager
    def get_connection(cls):
        """
        Context manager to get a connection from the pool.

        Yields:
            psycopg2 connection object

        Raises:
            OperationalError: If pool is not initialized or connection fails
        """
        if cls._pool is None:
            raise OperationalError("Database pool not initialized. Call initialize() first.")

        conn = None
        try:
            conn = cls._pool.getconn()
            yield conn
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                cls._pool.putconn(conn)

    @classmethod
    @contextmanager
    def get_cursor(cls, commit: bool = True):
        """
        Context manager to get a cursor for direct SQL execution.

        Args:
            commit: Whether to auto-commit on success

        Yields:
            psycopg2 cursor object

        Example:
            with DatabaseConnection.get_cursor() as cursor:
                cursor.execute("SELECT * FROM students")
                results = cursor.fetchall()
        """
        with cls.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                if commit:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                raise
            finally:
                cursor.close()

    @classmethod
    def execute_query(cls, query: str, params: Optional[tuple] = None) -> list:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            List of result rows
        """
        with cls.get_cursor(commit=False) as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()

    @classmethod
    def execute_update(cls, query: str, params: Optional[tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.

        Args:
            query: SQL query string
            params: Query parameters (optional)

        Returns:
            Number of rows affected
        """
        with cls.get_cursor(commit=True) as cursor:
            cursor.execute(query, params or ())
            return cursor.rowcount

    @classmethod
    def execute_many(cls, query: str, data: list) -> int:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query string with placeholders
            data: List of parameter tuples

        Returns:
            Total number of rows affected
        """
        with cls.get_cursor(commit=True) as cursor:
            cursor.executemany(query, data)
            return cursor.rowcount
