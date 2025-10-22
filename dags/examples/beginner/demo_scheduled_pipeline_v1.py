"""
Demo Scheduled Pipeline - Beginner Example

Demonstrates:
- Scheduled pipeline execution (daily at 2 AM)
- Retry configuration with exponential backoff
- Timeout handling
- Task dependencies
- Error logging with context

This is a P1 MVP example showing resilient pipeline execution.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from src.utils.logger import get_logger
from src.utils.retry_policies import create_retry_config
from src.utils.timeout_handler import create_timeout_config

logger = get_logger(__name__)

# DAG configuration
DAG_ID = "demo_scheduled_pipeline_v1"
SCHEDULE_INTERVAL = "0 2 * * *"  # Daily at 2 AM
START_DATE = datetime(2024, 1, 1)

# Retry configuration: 3 retries with exponential backoff
retry_config = create_retry_config(
    max_retries=3,
    strategy="exponential",
    base_delay=60,
    max_delay=600,  # 1 minute base  # 10 minutes max
)

# Timeout configuration: 30 minutes per task
timeout_config = create_timeout_config(timeout_seconds=1800)

# Default args combining retry and timeout
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    **retry_config,
    **timeout_config,
}

# Create DAG
dag = DAG(
    dag_id=DAG_ID,
    description="Scheduled ETL pipeline demonstrating retry and timeout handling",
    default_args=default_args,
    schedule=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    tags=["demo", "beginner", "scheduled", "retry", "timeout"],
)

# Task 1: Extract data from source
extract_data = BashOperator(
    task_id="extract_data",
    bash_command="""
    echo "============================================"
    echo "Task: Extract Data"
    echo "Execution Date: {{ ds }}"
    echo "Run ID: {{ run_id }}"
    echo "Try Number: {{ task_instance.try_number }}"
    echo "============================================"
    echo "Extracting data from source system..."
    sleep 2
    echo "Data extraction completed successfully"
    echo "Records extracted: 10000"
    """,
    dag=dag,
)

# Task 2: Validate extracted data
validate_data = BashOperator(
    task_id="validate_data",
    bash_command="""
    echo "============================================"
    echo "Task: Validate Data"
    echo "Execution Date: {{ ds }}"
    echo "Try Number: {{ task_instance.try_number }}"
    echo "============================================"
    echo "Running data quality checks..."
    sleep 1
    echo "✓ Schema validation passed"
    echo "✓ Null check passed"
    echo "✓ Duplicate check passed"
    echo "Data validation completed successfully"
    """,
    dag=dag,
)

# Task 3: Transform data
transform_data = BashOperator(
    task_id="transform_data",
    bash_command="""
    echo "============================================"
    echo "Task: Transform Data"
    echo "Execution Date: {{ ds }}"
    echo "Try Number: {{ task_instance.try_number }}"
    echo "============================================"
    echo "Applying business logic transformations..."
    sleep 3
    echo "✓ Applied aggregations"
    echo "✓ Calculated derived fields"
    echo "✓ Applied business rules"
    echo "Transformation completed successfully"
    """,
    dag=dag,
)

# Task 4: Load data to warehouse
load_data = BashOperator(
    task_id="load_data",
    bash_command="""
    echo "============================================"
    echo "Task: Load Data"
    echo "Execution Date: {{ ds }}"
    echo "Try Number: {{ task_instance.try_number }}"
    echo "============================================"
    echo "Loading data to warehouse..."
    sleep 2
    echo "✓ Connected to warehouse"
    echo "✓ Loaded 10000 records"
    echo "✓ Updated metadata"
    echo "Data load completed successfully"
    """,
    dag=dag,
)

# Task 5: Verify load success
verify_load = BashOperator(
    task_id="verify_load",
    bash_command="""
    echo "============================================"
    echo "Task: Verify Load"
    echo "Execution Date: {{ ds }}"
    echo "Try Number: {{ task_instance.try_number }}"
    echo "============================================"
    echo "Verifying data load..."
    sleep 1
    echo "✓ Row count matches"
    echo "✓ Data quality checks passed"
    echo "✓ Referential integrity maintained"
    echo "Verification completed successfully"
    echo ""
    echo "Pipeline execution completed successfully!"
    """,
    dag=dag,
)

# Define task dependencies (linear pipeline)
extract_data >> validate_data >> transform_data >> load_data >> verify_load

# Log DAG creation
logger.info(
    "DAG created",
    dag_id=DAG_ID,
    schedule=SCHEDULE_INTERVAL,
    retries=retry_config["retries"],
    timeout_seconds=1800,
    tasks=5,
)
