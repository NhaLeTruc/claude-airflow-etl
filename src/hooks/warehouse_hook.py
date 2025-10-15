"""
Warehouse database hook for Apache Airflow ETL Demo Platform.

Provides connection management and query execution for the mock data warehouse.
"""

from typing import Any, Dict, List, Optional

import psycopg2
from airflow.hooks.base import BaseHook
from psycopg2.extras import DictCursor, execute_values

from src.utils.config_loader import get_config_loader
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WarehouseHook(BaseHook):
    """
    Custom hook for warehouse database connections.

    Extends Airflow's BaseHook to provide PostgreSQL connection management
    with query execution, bulk inserts, and transaction support.
    """

    conn_name_attr = "warehouse_conn_id"
    default_conn_name = "warehouse_default"
    conn_type = "postgres"
    hook_name = "Warehouse Database"

    def __init__(self, warehouse_conn_id: str = "warehouse_default") -> None:
        """
        Initialize warehouse hook.

        Args:
            warehouse_conn_id: Airflow connection ID for warehouse database
        """
        super().__init__()
        self.warehouse_conn_id = warehouse_conn_id
        self._connection: Optional[psycopg2.extensions.connection] = None
        logger.debug("WarehouseHook initialized", conn_id=warehouse_conn_id)

    def get_conn(self) -> psycopg2.extensions.connection:
        """
        Get or create database connection.

        Returns:
            Active psycopg2 connection

        Raises:
            Exception: If connection fails
        """
        if self._connection is not None and not self._connection.closed:
            return self._connection

        try:
            # Try to get connection from Airflow connection
            conn = self.get_connection(self.warehouse_conn_id)
            self._connection = psycopg2.connect(
                host=conn.host,
                port=conn.port or 5432,
                database=conn.schema,
                user=conn.login,
                password=conn.password,
            )
            logger.info("Connected to warehouse database", host=conn.host, db=conn.schema)
        except Exception:
            # Fallback to environment variables
            logger.warning(
                "Airflow connection not found, using environment variables",
                conn_id=self.warehouse_conn_id,
            )
            config = get_config_loader().load_warehouse_config()
            self._connection = psycopg2.connect(
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["user"],
                password=config["password"],
            )
            logger.info(
                "Connected to warehouse database via env vars",
                host=config["host"],
                db=config["database"],
            )

        return self._connection

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None, fetch: bool = False
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute SQL query with optional parameter binding.

        Args:
            query: SQL query string
            params: Query parameters for safe binding
            fetch: Whether to fetch and return results

        Returns:
            List of result dicts if fetch=True, otherwise None

        Raises:
            Exception: If query execution fails
        """
        conn = self.get_conn()
        cursor = None

        try:
            cursor = conn.cursor(cursor_factory=DictCursor)
            cursor.execute(query, params)

            if fetch:
                results = [dict(row) for row in cursor.fetchall()]
                logger.debug("Query executed successfully", rows=len(results))
                return results

            conn.commit()
            affected_rows = cursor.rowcount
            logger.debug("Query executed successfully", affected_rows=affected_rows)
            return None

        except Exception as e:
            conn.rollback()
            logger.error("Query execution failed", error=str(e), query=query[:100])
            raise

        finally:
            if cursor:
                cursor.close()

    def bulk_insert(
        self, table: str, records: List[Dict[str, Any]], page_size: int = 1000
    ) -> int:
        """
        Bulk insert records into table using execute_values.

        Args:
            table: Table name (with schema if needed)
            records: List of record dicts with column: value pairs
            page_size: Number of records to insert per batch

        Returns:
            Total number of records inserted

        Raises:
            ValueError: If records list is empty
            Exception: If insert fails
        """
        if not records:
            msg = "Cannot bulk insert empty records list"
            logger.error(msg)
            raise ValueError(msg)

        conn = self.get_conn()
        cursor = None

        try:
            cursor = conn.cursor()

            # Get column names from first record
            columns = list(records[0].keys())
            column_str = ", ".join(columns)
            placeholder = f"({', '.join(['%s'] * len(columns))})"

            # Prepare data tuples
            data = [tuple(record[col] for col in columns) for record in records]

            # Execute bulk insert
            query = f"INSERT INTO {table} ({column_str}) VALUES %s"
            execute_values(cursor, query, data, template=placeholder, page_size=page_size)

            conn.commit()
            inserted_count = len(records)
            logger.info("Bulk insert completed", table=table, rows=inserted_count)
            return inserted_count

        except Exception as e:
            conn.rollback()
            logger.error("Bulk insert failed", table=table, error=str(e))
            raise

        finally:
            if cursor:
                cursor.close()

    def get_pandas_df(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute query and return results as pandas DataFrame.

        Args:
            query: SQL query string
            params: Query parameters for safe binding

        Returns:
            pandas DataFrame with query results

        Raises:
            ImportError: If pandas not installed
            Exception: If query execution fails
        """
        try:
            import pandas as pd
        except ImportError as e:
            msg = "pandas is required for get_pandas_df"
            logger.error(msg)
            raise ImportError(msg) from e

        conn = self.get_conn()
        try:
            df = pd.read_sql_query(query, conn, params=params)
            logger.debug("Query executed to DataFrame", rows=len(df))
            return df
        except Exception as e:
            logger.error("DataFrame query failed", error=str(e), query=query[:100])
            raise

    def close(self) -> None:
        """Close database connection if open."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.debug("Warehouse connection closed")
            self._connection = None
