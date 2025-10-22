"""
Simple Extract-Load DAG - Beginner Example

PATTERN: Basic ETL - Extract data from source and load to warehouse staging table

LEARNING OBJECTIVES:
1. Understand basic DAG structure and configuration
2. Learn PostgresOperator for database operations
3. Practice simple extract-load pattern with truncate-and-load for idempotency
4. Use Airflow templating with execution_date

USE CASE:
Extract customer dimension data from source warehouse and load to staging table
for downstream processing. This is a full refresh pattern - truncate before load
to ensure idempotency.

KEY AIRFLOW FEATURES:
- Basic DAG definition with default_args
- PostgresOperator for SQL execution
- Task dependencies with >>
- Jinja templating with {{ ds }}
- Airflow connections for database access
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow_demo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="demo_simple_extract_load_v1",
    default_args=default_args,
    description="Simple extract-load pattern with truncate-and-load for idempotency",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["beginner", "extract-load", "postgres", "idempotent"],
    doc_md=__doc__,
)

# Task 1: Truncate staging table for idempotent full refresh
truncate_staging = PostgresOperator(
    task_id="truncate_staging_table",
    postgres_conn_id="warehouse",
    sql="""
    TRUNCATE TABLE staging.dim_customer_staging;
    """,
    dag=dag,
)

# Task 2: Extract and load customer data from source to staging
extract_load_customers = PostgresOperator(
    task_id="extract_load_customers",
    postgres_conn_id="warehouse",
    sql="""
    INSERT INTO staging.dim_customer_staging (
        customer_id,
        customer_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        country,
        created_date,
        last_modified_date
    )
    SELECT
        customer_id,
        customer_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        country,
        created_date,
        last_modified_date
    FROM warehouse.dim_customer
    WHERE last_modified_date <= '{{ ds }}'::date;
    """,
    dag=dag,
)

# Task 3: Log extraction statistics
log_extraction_stats = PostgresOperator(
    task_id="log_extraction_stats",
    postgres_conn_id="warehouse",
    sql="""
    INSERT INTO etl_metadata.load_log (
        pipeline_name,
        execution_date,
        table_name,
        rows_loaded,
        load_timestamp
    )
    SELECT
        'demo_simple_extract_load_v1',
        '{{ ds }}'::date,
        'staging.dim_customer_staging',
        COUNT(*),
        CURRENT_TIMESTAMP
    FROM staging.dim_customer_staging;
    """,
    dag=dag,
)

# Define task dependencies
# Pattern: Truncate → Extract/Load → Log Stats
truncate_staging >> extract_load_customers >> log_extraction_stats

"""
PATTERN EXPLANATION:

1. TRUNCATE-AND-LOAD PATTERN:
   - Truncate staging table first to ensure clean slate
   - Load all data from source (full refresh)
   - Idempotent: Running multiple times with same execution_date produces same result

2. WHY THIS PATTERN:
   - Simple and reliable for full dimension refreshes
   - Easy to understand and debug
   - No need to track deletes or updates
   - Staging table size manageable for daily full loads

3. WHEN TO USE:
   - Small to medium dimension tables (< 10M rows)
   - Daily full refreshes acceptable
   - Downstream processing can handle full table scans
   - Don't need incremental loading complexity

4. WHEN NOT TO USE:
   - Large tables where full refresh is expensive
   - Need real-time or streaming updates
   - Source system doesn't support full extracts
   - Historical tracking required (use SCD Type 2 instead)

5. IDEMPOTENCY:
   - Execution_date = 2025-01-15 always loads data <= 2025-01-15
   - Truncate ensures no duplicates from previous runs
   - Rerunning produces identical results

6. NEXT STEPS:
   - Learn incremental loading (demo_incremental_load_v1)
   - Add data quality checks (demo_data_quality_basics_v1)
   - Add notifications on failure (demo_notification_basics_v1)
   - Add retry logic and timeouts (demo_scheduled_pipeline_v1)
"""
