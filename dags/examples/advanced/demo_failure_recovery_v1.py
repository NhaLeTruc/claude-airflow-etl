"""
Demo Failure Recovery Pipeline - Advanced Example

Demonstrates:
- Intentional task failures for testing
- Downstream task skipping on upstream failure
- Parallel task continuation when sibling fails
- Compensation/cleanup logic
- Comprehensive error logging
- Various trigger rules

This is a P1 MVP example showing resilient pipeline execution patterns.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from src.utils.logger import get_logger
from src.utils.retry_policies import create_retry_config

logger = get_logger(__name__)

# DAG configuration
DAG_ID = "demo_failure_recovery_v1"
SCHEDULE_INTERVAL = None  # Manual trigger only
START_DATE = datetime(2024, 1, 1)

# Retry configuration: 2 retries with exponential backoff
retry_config = create_retry_config(max_retries=2, strategy="exponential", base_delay=30)

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    **retry_config,
}


def simulate_failure(**context):
    """Simulate a task failure for testing."""
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    execution_date = context["execution_date"]
    try_number = context["task_instance"].try_number

    logger.error(
        "Simulated failure",
        dag_id=dag_id,
        task_id=task_id,
        execution_date=str(execution_date),
        try_number=try_number,
        error_type="SimulatedError",
    )

    raise Exception(f"Simulated failure in {task_id} (attempt {try_number})")


def compensation_logic(**context):
    """Execute compensation/cleanup logic after failure."""
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]

    logger.info(
        "Executing compensation logic",
        dag_id=dag_id,
        execution_date=str(execution_date),
    )


# Create DAG
dag = DAG(
    dag_id=DAG_ID,
    description="Advanced pipeline demonstrating failure handling and recovery patterns",
    default_args=default_args,
    schedule=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    tags=["demo", "advanced", "failure-handling", "recovery"],
)

# === Parallel Branch A (Will fail) ===

extract_source_a = BashOperator(
    task_id="extract_source_a",
    bash_command="""
    echo "============================================"
    echo "Task: Extract Source A"
    echo "Execution Date: {{ ds }}"
    echo "Try Number: {{ task_instance.try_number }}"
    echo "============================================"
    echo "Extracting from Source A..."
    sleep 1
    echo "Extraction completed"
    """,
    dag=dag,
)

# This task will fail intentionally
failing_task = PythonOperator(
    task_id="failing_transform_a",
    python_callable=simulate_failure,
    provide_context=True,
    dag=dag,
)

# Downstream task - should be skipped due to upstream failure
load_source_a = BashOperator(
    task_id="load_source_a",
    bash_command="""
    echo "This task should be SKIPPED due to upstream failure"
    echo "If you see this, something is wrong!"
    """,
    dag=dag,
)

# === Parallel Branch B (Will succeed) ===

extract_source_b = BashOperator(
    task_id="extract_source_b",
    bash_command="""
    echo "============================================"
    echo "Task: Extract Source B"
    echo "Execution Date: {{ ds }}"
    echo "Try Number: {{ task_instance.try_number }}"
    echo "============================================"
    echo "Extracting from Source B..."
    sleep 1
    echo "Extraction completed"
    """,
    dag=dag,
)

transform_b = BashOperator(
    task_id="transform_b",
    bash_command="""
    echo "============================================"
    echo "Task: Transform B"
    echo "Execution Date: {{ ds }}"
    echo "============================================"
    echo "Transforming Source B data..."
    sleep 1
    echo "✓ Transformation completed successfully"
    echo "Note: Branch A failed, but Branch B continues!"
    """,
    dag=dag,
)

load_source_b = BashOperator(
    task_id="load_source_b",
    bash_command="""
    echo "============================================"
    echo "Task: Load Source B"
    echo "Execution Date: {{ ds }}"
    echo "============================================"
    echo "Loading Source B data..."
    sleep 1
    echo "✓ Load completed successfully"
    echo "Branch B completed despite Branch A failure!"
    """,
    dag=dag,
)

# === Merge point (runs on any completion) ===

merge_results = BashOperator(
    task_id="merge_results",
    bash_command="""
    echo "============================================"
    echo "Task: Merge Results"
    echo "Execution Date: {{ ds }}"
    echo "Trigger Rule: ONE_SUCCESS"
    echo "============================================"
    echo "Merging results from successful branches..."
    echo "Branch A status: FAILED"
    echo "Branch B status: SUCCESS"
    echo "Proceeding with partial results..."
    """,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Run if at least one upstream succeeded
    dag=dag,
)

# === Cleanup (runs regardless of failure) ===

cleanup = PythonOperator(
    task_id="cleanup_and_compensation",
    python_callable=compensation_logic,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,  # Run after all upstream tasks complete (success or fail)
    dag=dag,
)

# === Final status check ===

final_status = BashOperator(
    task_id="final_status",
    bash_command="""
    echo "============================================"
    echo "Task: Final Status"
    echo "Trigger Rule: ALL_DONE"
    echo "============================================"
    echo "Pipeline execution completed"
    echo ""
    echo "Expected Results:"
    echo "  ✗ extract_source_a → failing_transform_a → load_source_a (FAILED/SKIPPED)"
    echo "  ✓ extract_source_b → transform_b → load_source_b (SUCCESS)"
    echo "  ✓ merge_results (SUCCESS - ONE_SUCCESS rule)"
    echo "  ✓ cleanup_and_compensation (SUCCESS - ALL_DONE rule)"
    echo "  ✓ final_status (SUCCESS - ALL_DONE rule)"
    echo ""
    echo "Lessons Demonstrated:"
    echo "  1. Failures don't block independent parallel branches"
    echo "  2. Downstream tasks skip when upstream fails"
    echo "  3. Cleanup tasks run regardless of success/failure"
    echo "  4. Trigger rules provide fine-grained control"
    echo "  5. Error logging captures full context"
    """,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Define dependencies

# Branch A (will fail)
extract_source_a >> failing_task >> load_source_a

# Branch B (will succeed)
extract_source_b >> transform_b >> load_source_b

# Merge point waits for both branches
[load_source_a, load_source_b] >> merge_results

# Cleanup runs after merge
merge_results >> cleanup

# Final status runs after cleanup
cleanup >> final_status

# Log DAG creation
logger.info(
    "Advanced failure recovery DAG created",
    dag_id=DAG_ID,
    tasks=11,
    branches=2,
    trigger_rules=["ONE_SUCCESS", "ALL_DONE"],
    demonstrates=["failure handling", "parallel execution", "compensation logic", "trigger rules"],
)
