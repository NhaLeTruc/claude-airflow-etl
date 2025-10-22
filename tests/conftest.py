"""
Pytest configuration and shared fixtures for Apache Airflow ETL Demo Platform.

Provides fixtures for database connections, mock data, and testing utilities.
"""

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from airflow.models import DagBag
from airflow.utils.state import State

from src.utils.data_generator import MockDataGenerator


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """
    Fixture providing path to test data directory.

    Returns:
        Path to tests/fixtures directory
    """
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def mock_dag_configs_dir(test_data_dir: Path) -> Path:
    """
    Fixture providing path to mock DAG configurations.

    Returns:
        Path to tests/fixtures/mock_configs directory
    """
    return test_data_dir / "mock_configs"


@pytest.fixture(scope="session")
def mock_datasets_dir(test_data_dir: Path) -> Path:
    """
    Fixture providing path to mock datasets.

    Returns:
        Path to tests/fixtures/mock_datasets directory
    """
    return test_data_dir / "mock_datasets"


@pytest.fixture
def mock_data_generator() -> MockDataGenerator:
    """
    Fixture providing MockDataGenerator instance with deterministic seed.

    Returns:
        MockDataGenerator with seed=42 for reproducible tests
    """
    return MockDataGenerator(seed=42)


@pytest.fixture
def sample_customers(mock_data_generator: MockDataGenerator) -> list[dict[str, Any]]:
    """
    Fixture providing sample customer data.

    Returns:
        List of 100 customer dicts
    """
    return mock_data_generator.generate_customers(count=100)


@pytest.fixture
def sample_products(mock_data_generator: MockDataGenerator) -> list[dict[str, Any]]:
    """
    Fixture providing sample product data.

    Returns:
        List of 50 product dicts
    """
    return mock_data_generator.generate_products(count=50)


@pytest.fixture
def sample_sales(
    mock_data_generator: MockDataGenerator,
    sample_customers: list[dict[str, Any]],
    sample_products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Fixture providing sample sales data with quality issues.

    Returns:
        List of 500 sales dicts with intentional quality issues
    """
    customer_ids = [c["customer_id"] for c in sample_customers]
    product_ids = [p["product_id"] for p in sample_products]
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    return mock_data_generator.generate_sales(
        count=500,
        customer_ids=customer_ids,
        product_ids=product_ids,
        date_range=(start_date, end_date),
    )


@pytest.fixture
def sample_dag_config() -> dict[str, Any]:
    """
    Fixture providing sample DAG configuration.

    Returns:
        Valid DAG configuration dict
    """
    return {
        "dag_id": "test_etl_pipeline",
        "description": "Test ETL pipeline for unit tests",
        "schedule_interval": "0 2 * * *",
        "start_date": "2024-01-01",
        "default_args": {
            "owner": "airflow",
            "retries": 2,
            "retry_delay_minutes": 5,
            "email_on_failure": False,
            "email_on_retry": False,
        },
        "tasks": [
            {
                "task_id": "extract_data",
                "operator": "BashOperator",
                "params": {"bash_command": "echo 'Extracting data'"},
            },
            {
                "task_id": "transform_data",
                "operator": "PythonOperator",
                "params": {"python_callable": "transform_function"},
                "dependencies": ["extract_data"],
            },
            {
                "task_id": "load_data",
                "operator": "BashOperator",
                "params": {"bash_command": "echo 'Loading data'"},
                "dependencies": ["transform_data"],
            },
        ],
    }


@pytest.fixture
def mock_warehouse_connection() -> dict[str, Any]:
    """
    Fixture providing mock warehouse connection details.

    Returns:
        Connection config dict
    """
    return {
        "host": "localhost",
        "port": 5433,
        "database": "warehouse_test",
        "user": "test_user",
        "password": "test_password",
    }


@pytest.fixture
def mock_airflow_context() -> dict[str, Any]:
    """
    Fixture providing mock Airflow task context.

    Returns:
        Context dict with dag_run, task_instance, execution_date, etc.
    """
    execution_date = datetime(2024, 10, 15, 12, 0, 0)
    return {
        "dag": Mock(dag_id="test_dag"),
        "task": Mock(task_id="test_task"),
        "dag_run": Mock(
            run_id="test_run",
            execution_date=execution_date,
            start_date=execution_date,
        ),
        "task_instance": Mock(
            task_id="test_task",
            dag_id="test_dag",
            execution_date=execution_date,
            state=State.RUNNING,
            try_number=1,
        ),
        "execution_date": execution_date,
        "ds": execution_date.strftime("%Y-%m-%d"),
        "ds_nodash": execution_date.strftime("%Y%m%d"),
        "params": {},
        "var": {"value": {}, "json": {}},
        "conf": {},
    }


@pytest.fixture
def mock_smtp_config() -> dict[str, Any]:
    """
    Fixture providing mock SMTP configuration.

    Returns:
        SMTP config dict
    """
    return {
        "host": "smtp.example.com",
        "port": 587,
        "user": "test@example.com",
        "password": "test_password",
        "from_email": "airflow@example.com",
        "use_tls": True,
    }


@pytest.fixture
def mock_teams_webhook_url() -> str:
    """
    Fixture providing mock Teams webhook URL.

    Returns:
        Fake webhook URL string
    """
    return "https://example.webhook.office.com/webhookb2/test"


@pytest.fixture
def mock_telegram_config() -> dict[str, str]:
    """
    Fixture providing mock Telegram configuration.

    Returns:
        Telegram config dict
    """
    return {
        "bot_token": "1234567890:ABCdefGHIjklMNOpqrsTUVwxyz",
        "chat_id": "-1001234567890",
    }


@pytest.fixture
def mock_quality_check_result() -> dict[str, Any]:
    """
    Fixture providing mock quality check result.

    Returns:
        Quality check result dict
    """
    return {
        "check_id": "chk_001",
        "check_name": "null_check_customer_email",
        "check_type": "NULL_CHECK",
        "severity": "WARNING",
        "passed": False,
        "records_checked": 1000,
        "records_failed": 20,
        "failure_rate": 0.02,
        "threshold": 0.01,
        "message": "Null rate 2.00% exceeds threshold 1.00%",
        "execution_time": 0.125,
    }


@pytest.fixture
def mock_spark_config() -> dict[str, Any]:
    """
    Fixture providing mock Spark configuration.

    Returns:
        Spark config dict
    """
    return {
        "master": "local[*]",
        "app_name": "test_spark_job",
        "conf": {
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.sql.shuffle.partitions": "10",
        },
    }


@pytest.fixture(autouse=True)
def set_test_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Auto-use fixture to set test environment variables.

    Sets up clean test environment for all tests.
    """
    monkeypatch.setenv("AIRFLOW_HOME", "/tmp/airflow_test")
    monkeypatch.setenv("WAREHOUSE_HOST", "localhost")
    monkeypatch.setenv("WAREHOUSE_PORT", "5433")
    monkeypatch.setenv("WAREHOUSE_DB", "warehouse_test")
    monkeypatch.setenv("WAREHOUSE_USER", "test_user")
    monkeypatch.setenv("WAREHOUSE_PASSWORD", "test_password")


@pytest.fixture
def dag_bag() -> DagBag:
    """
    Fixture providing DagBag for DAG validation tests.

    Returns:
        DagBag instance loaded with test DAGs

    Note:
        This may be slow for large DAG collections
    """
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture
def mock_psycopg2_connection() -> MagicMock:
    """
    Fixture providing mock psycopg2 connection.

    Returns:
        MagicMock psycopg2 connection with cursor support
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.closed = False
    return mock_conn


@pytest.fixture
def mock_psycopg2_cursor(mock_psycopg2_connection: MagicMock) -> MagicMock:
    """
    Fixture providing mock psycopg2 cursor.

    Returns:
        MagicMock cursor from mock connection
    """
    return mock_psycopg2_connection.cursor.return_value
