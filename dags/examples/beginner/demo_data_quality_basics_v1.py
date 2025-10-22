"""
Data Quality Basics DAG - Beginner Example

PATTERN: Basic data quality validation after ETL load

LEARNING OBJECTIVES:
1. Understand importance of data quality checks in ETL pipelines
2. Learn to use custom quality check operators
3. Practice fail-fast pattern to prevent bad data propagation
4. Configure severity levels for different quality issues

USE CASE:
After loading customer data to warehouse, validate schema and completeness
before allowing downstream processing. This ensures data quality issues
are caught early and prevent cascading failures.

KEY AIRFLOW FEATURES:
- Custom quality check operators (Schema, Completeness)
- Trigger rules for conditional execution
- Quality severity levels (WARNING vs CRITICAL)
- XCom for passing quality check results
- Branching based on quality check outcomes
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def days_ago(n):
    return datetime.now() - timedelta(days=n)

from src.operators.quality.base_quality_operator import QualitySeverity
from src.operators.quality.completeness_checker import CompletenessChecker
from src.operators.quality.schema_validator import SchemaValidator
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Default arguments
default_args = {
    "owner": "data_quality_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="demo_data_quality_basics_v1",
    default_args=default_args,
    description="Basic data quality validation with schema and completeness checks",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["beginner", "data-quality", "validation", "fail-fast"],
    doc_md=__doc__,
)

# Task 1: Load customer data to warehouse (simulated)
load_customer_data = PostgresOperator(
    task_id="load_customer_data",
    postgres_conn_id="warehouse",
    sql="""
    -- Simulate loading customer data
    -- In real scenario, this would be your ETL load task
    INSERT INTO warehouse.dim_customer (customer_id, customer_name, email, created_date)
    SELECT
        generate_series(1, 100) AS customer_id,
        'Customer ' || generate_series(1, 100) AS customer_name,
        'customer' || generate_series(1, 100) || '@example.com' AS email,
        '{{ ds }}'::date AS created_date
    ON CONFLICT (customer_id) DO UPDATE
    SET customer_name = EXCLUDED.customer_name,
        email = EXCLUDED.email,
        last_modified_date = CURRENT_TIMESTAMP;
    """,
    dag=dag,
)

# Task 2: Schema validation (CRITICAL severity - fails pipeline if schema wrong)
validate_schema = SchemaValidator(
    task_id="validate_customer_schema",
    warehouse_conn_id="warehouse",
    table_name="warehouse.dim_customer",
    expected_schema=[
        {"column_name": "customer_id", "data_type": "integer", "nullable": False},
        {"column_name": "customer_name", "data_type": "character varying", "nullable": True},
        {"column_name": "email", "data_type": "character varying", "nullable": True},
        {"column_name": "phone", "data_type": "character varying", "nullable": True},
        {"column_name": "address", "data_type": "character varying", "nullable": True},
        {"column_name": "city", "data_type": "character varying", "nullable": True},
        {"column_name": "state", "data_type": "character varying", "nullable": True},
        {"column_name": "zip_code", "data_type": "character varying", "nullable": True},
        {"column_name": "country", "data_type": "character varying", "nullable": True},
        {"column_name": "created_date", "data_type": "date", "nullable": True},
        {
            "column_name": "last_modified_date",
            "data_type": "timestamp without time zone",
            "nullable": True,
        },
    ],
    severity=QualitySeverity.CRITICAL,  # Fail pipeline on schema mismatch
    check_extra_columns=False,  # Allow extra columns (flexible)
    dag=dag,
)

# Task 3: Completeness check (WARNING severity - logs but doesn't fail)
check_row_count = CompletenessChecker(
    task_id="check_customer_completeness",
    warehouse_conn_id="warehouse",
    table_name="warehouse.dim_customer",
    min_count=10,  # Expect at least 10 customers
    expected_count=1000,  # Expect around 1000 customers
    tolerance_percentage=20.0,  # Allow 20% deviation from expected
    severity=QualitySeverity.WARNING,  # Warn but don't fail
    dag=dag,
)


# Task 4: Log quality check results
def log_quality_results(**context):
    """Log quality check results for monitoring."""
    ti = context["task_instance"]

    # Get schema validation result
    schema_result = ti.xcom_pull(task_ids="validate_customer_schema")
    logger.info(f"Schema validation result: {schema_result}")

    # Get completeness check result
    completeness_result = ti.xcom_pull(task_ids="check_customer_completeness")
    logger.info(f"Completeness check result: {completeness_result}")

    # Log summary
    logger.info(
        "Quality checks completed",
        schema_passed=schema_result.get("passed", False) if schema_result else False,
        completeness_passed=(
            completeness_result.get("passed", False) if completeness_result else False
        ),
        execution_date=context["ds"],
    )


log_results = PythonOperator(
    task_id="log_quality_results",
    python_callable=log_quality_results,
    provide_context=True,
    dag=dag,
)

# Task 5: Quality checks passed - proceed with downstream processing
quality_passed = EmptyOperator(
    task_id="quality_checks_passed",
    dag=dag,
)

# Task 6: Downstream processing (only runs if quality passed)
process_customer_data = PostgresOperator(
    task_id="process_customer_data",
    postgres_conn_id="warehouse",
    sql="""
    -- Simulate downstream processing
    -- This could be aggregations, joins, etc.
    SELECT
        COUNT(*) AS total_customers,
        COUNT(email) AS customers_with_email,
        COUNT(DISTINCT country) AS countries
    FROM warehouse.dim_customer;
    """,
    dag=dag,
)

# Define task dependencies
# Pattern: Load → Validate (Schema + Completeness) → Log → Proceed if passed → Process
load_customer_data >> [validate_schema, check_row_count]
[validate_schema, check_row_count] >> log_results
log_results >> quality_passed >> process_customer_data

"""
PATTERN EXPLANATION:

1. FAIL-FAST DATA QUALITY PATTERN:
   - Run quality checks immediately after data load
   - Block downstream processing if critical checks fail
   - Prevent bad data from propagating through pipeline

2. SEVERITY LEVELS:
   - CRITICAL (schema validation): Pipeline fails immediately
   - WARNING (completeness): Logs issue but allows processing
   - Choose severity based on business impact

3. WHY THIS PATTERN:
   - Catch data quality issues early in pipeline
   - Prevent cascading failures in downstream tasks
   - Provide clear audit trail of quality checks
   - Enable automated quality monitoring

4. WHEN TO USE:
   - After any data load or transformation
   - Before expensive downstream processing
   - When data quality SLAs are critical
   - For regulated data (compliance requirements)

5. QUALITY CHECKS EXPLAINED:

   SCHEMA VALIDATION:
   - Verifies expected columns exist
   - Checks data types match specification
   - Ensures nullable constraints are correct
   - Critical because schema mismatches break queries

   COMPLETENESS CHECK:
   - Verifies minimum row count threshold
   - Checks expected count within tolerance
   - Detects incomplete data loads
   - Warning level allows processing with partial data

6. NEXT STEPS:
   - Add more quality checks (freshness, uniqueness, null rate)
   - Implement quality check result storage
   - Add notifications for quality failures
   - Learn comprehensive quality checks (demo_comprehensive_quality_v1)

7. BEST PRACTICES:
   - Always validate schema after structure changes
   - Set appropriate severity levels per business rules
   - Log all quality check results for audit trail
   - Use quality checks as task dependencies
   - Configure alerts for quality failures
   - Review quality metrics regularly

8. COMMON PITFALLS TO AVOID:
   - Don't skip quality checks to save time (technical debt)
   - Don't use CRITICAL for all checks (too strict)
   - Don't ignore WARNING-level failures (monitor trends)
   - Don't run quality checks after downstream processing (too late)
"""
