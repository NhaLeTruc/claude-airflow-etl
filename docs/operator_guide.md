# Operator Guide: Retry and Failure Handling Patterns

Complete guide to implementing robust retry and failure handling patterns in Apache Airflow ETL pipelines.

## Table of Contents

- [Overview](#overview)
- [Retry Strategies](#retry-strategies)
- [Timeout Management](#timeout-management)
- [Failure Handling](#failure-handling)
- [Trigger Rules](#trigger-rules)
- [Compensation Logic](#compensation-logic)
- [Error Logging](#error-logging)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Overview

Resilient pipelines handle failures gracefully through:
- **Automatic retries** with configurable backoff strategies
- **Timeout enforcement** to prevent hung tasks
- **Failure propagation** control via trigger rules
- **Compensation logic** for cleanup and rollback
- **Comprehensive error logging** for debugging

## Retry Strategies

### Exponential Backoff (Recommended)

Doubles the delay between each retry attempt.

**When to use**: Most scenarios, especially external API calls and database operations

**Configuration**:

```python
from src.utils.retry_policies import create_retry_config

retry_config = create_retry_config(
    max_retries=3,
    strategy="exponential",
    base_delay=60,  # 1 minute
    max_delay=600   # 10 minutes cap
)

# Retry delays: 60s → 120s → 240s → 480s (capped at 600s)
```

**Advantages**:
- Reduces load on failing systems
- Gives systems time to recover
- Standard industry practice

**Use in default_args**:

```python
default_args = {
    "owner": "data_team",
    **retry_config,
}
```

### Linear Backoff

Increases delay by a fixed amount each retry.

**When to use**: Predictable recovery times, rate limiting scenarios

**Configuration**:

```python
retry_config = create_retry_config(
    max_retries=4,
    strategy="linear",
    base_delay=60
)

# Retry delays: 60s → 120s → 180s → 240s
```

**Advantages**:
- Predictable retry schedule
- Gentler increase than exponential

### Fixed Backoff

Same delay for all retry attempts.

**When to use**: Known fixed recovery windows, simple retry logic

**Configuration**:

```python
retry_config = create_retry_config(
    max_retries=5,
    strategy="fixed",
    base_delay=120  # Always 2 minutes
)

# Retry delays: 120s → 120s → 120s → 120s → 120s
```

**Advantages**:
- Simplest to understand
- Consistent behavior

## Timeout Management

### Task-Level Timeouts

Prevent tasks from running indefinitely.

**Configuration**:

```python
from src.utils.timeout_handler import create_timeout_config

timeout_config = create_timeout_config(
    timeout_seconds=1800  # 30 minutes
)

default_args = {
    "owner": "data_team",
    **timeout_config,
}
```

### Timeout with Callback

Execute custom logic when timeout occurs.

```python
def on_timeout_callback(context):
    """Called when task times out."""
    task_id = context["task"].task_id
    execution_date = context["execution_date"]

    logger.error(
        "Task timeout",
        task_id=task_id,
        execution_date=str(execution_date)
    )

    # Send alert, cleanup resources, etc.

timeout_config = create_timeout_config(
    timeout_seconds=1800,
    on_timeout_callback=on_timeout_callback
)
```

### Monitoring Timeout Approaching

Warn before timeout is reached.

```python
from src.utils.timeout_handler import should_warn_about_timeout

start_time = task_instance.start_date
current_time = datetime.now()

if should_warn_about_timeout(start_time, current_time, 1800, threshold=0.8):
    logger.warning("Task approaching timeout (80% elapsed)")
```

## Failure Handling

### Downstream Task Skipping

By default, downstream tasks skip when upstream fails.

```python
# Task A fails → Task B automatically skips
task_a >> task_b
```

**Behavior**:
- Task B state: `SKIPPED`
- Task B doesn't consume resources
- Failure doesn't propagate further

### Parallel Task Independence

Parallel tasks continue even if siblings fail.

```python
# If task_a fails, task_b still runs
[task_a, task_b] >> task_c
```

**Behavior**:
- Task A fails → Task B continues
- Task C waits for both (may skip if default trigger)

### Failure Callbacks

Execute custom logic on task failure.

```python
def on_failure_callback(context):
    """Called when task fails after all retries."""
    task_id = context["task"].task_id
    exception = context.get("exception")
    try_number = context["task_instance"].try_number

    logger.error(
        "Task failed permanently",
        task_id=task_id,
        try_number=try_number,
        exception=str(exception),
        error_type=type(exception).__name__
    )

    # Send alerts, create tickets, etc.

default_args = {
    "on_failure_callback": on_failure_callback,
}
```

### Retry Callbacks

Execute logic on each retry attempt.

```python
def on_retry_callback(context):
    """Called when task is retried."""
    task_id = context["task"].task_id
    try_number = context["task_instance"].try_number
    max_tries = context["task_instance"].max_tries

    logger.warning(
        "Task retrying",
        task_id=task_id,
        try_number=try_number,
        max_tries=max_tries
    )

default_args = {
    "on_retry_callback": on_retry_callback,
}
```

## Trigger Rules

Control when tasks execute based on upstream states.

### ALL_SUCCESS (Default)

Task runs only if ALL upstream tasks succeeded.

```python
from airflow.utils.trigger_rule import TriggerRule

task = BashOperator(
    task_id="task",
    bash_command="echo 'All upstream succeeded'",
    trigger_rule=TriggerRule.ALL_SUCCESS,  # Default
)
```

### ONE_SUCCESS

Task runs if AT LEAST ONE upstream task succeeded.

```python
task = BashOperator(
    task_id="merge_results",
    bash_command="echo 'At least one branch succeeded'",
    trigger_rule=TriggerRule.ONE_SUCCESS,
)

# Use for: Partial result processing
```

### ALL_FAILED

Task runs only if ALL upstream tasks failed.

```python
task = BashOperator(
    task_id="send_failure_alert",
    bash_command="echo 'Everything failed'",
    trigger_rule=TriggerRule.ALL_FAILED,
)

# Use for: Global failure handling
```

### ALL_DONE

Task runs after ALL upstream tasks complete (success OR failure).

```python
task = BashOperator(
    task_id="cleanup",
    bash_command="echo 'Cleaning up resources'",
    trigger_rule=TriggerRule.ALL_DONE,
)

# Use for: Cleanup, final status checks
```

### ONE_FAILED

Task runs if AT LEAST ONE upstream task failed.

```python
task = BashOperator(
    task_id="partial_failure_handler",
    bash_command="echo 'Something went wrong'",
    trigger_rule=TriggerRule.ONE_FAILED,
)
```

### NONE_FAILED

Task runs if NO upstream tasks failed (skipped = ok).

```python
task = BashOperator(
    task_id="conditional_task",
    bash_command="echo 'No failures detected'",
    trigger_rule=TriggerRule.NONE_FAILED,
)
```

## Compensation Logic

### Cleanup Tasks

Always run cleanup regardless of success/failure.

```python
def cleanup_resources(**context):
    """Cleanup temporary files, connections, locks."""
    logger.info("Executing cleanup")

    # Release locks
    # Delete temporary files
    # Close connections
    # Update status tables

cleanup = PythonOperator(
    task_id="cleanup",
    python_callable=cleanup_resources,
    trigger_rule=TriggerRule.ALL_DONE,  # Always run
)

main_tasks >> cleanup
```

### Rollback Logic

Undo partial changes on failure.

```python
def rollback_changes(**context):
    """Rollback database changes on failure."""
    execution_date = context["execution_date"]

    logger.warning("Rolling back changes", execution_date=str(execution_date))

    # Delete partially loaded data
    # Restore backups
    # Reset state flags

rollback = PythonOperator(
    task_id="rollback",
    python_callable=rollback_changes,
    trigger_rule=TriggerRule.ONE_FAILED,  # Only on failure
)

[extract, transform, load] >> rollback
```

### Compensation Transactions

Execute compensating actions for failed operations.

```python
def compensate_failed_transfer(**context):
    """Compensate for failed data transfer."""
    logger.info("Executing compensation logic")

    # Reverse credited amounts
    # Send reversal notifications
    # Update audit logs

compensate = PythonOperator(
    task_id="compensate",
    python_callable=compensate_failed_transfer,
    trigger_rule=TriggerRule.ALL_FAILED,
)
```

## Error Logging

### Structured Logging with Context

Include full execution context in error logs.

```python
from src.utils.logger import get_logger

logger = get_logger(__name__)

def task_function(**context):
    # Add context
    logger.add_context(
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
        execution_date=str(context["execution_date"]),
        run_id=context["run_id"],
        try_number=context["task_instance"].try_number
    )

    try:
        # Task logic
        logger.info("Task started")
        result = perform_operation()
        logger.info("Task completed", result=result)

    except Exception as e:
        logger.exception(
            "Task failed",
            error_type=type(e).__name__,
            error_message=str(e)
        )
        raise
```

### Error Context in Bash Tasks

Log context in bash commands.

```bash
bash_command="""
echo "============================================"
echo "Task: {{ task.task_id }}"
echo "DAG: {{ dag.dag_id }}"
echo "Execution Date: {{ ds }}"
echo "Run ID: {{ run_id }}"
echo "Try Number: {{ task_instance.try_number }}"
echo "Max Tries: {{ task_instance.max_tries }}"
echo "============================================"

# Your task logic here

if [ $? -ne 0 ]; then
    echo "ERROR: Task failed"
    echo "Error code: $?"
    exit 1
fi
"""
```

## Best Practices

### 1. Choose Appropriate Retry Strategy

- **External APIs**: Exponential backoff (reduces load)
- **Database operations**: Exponential backoff (connection recovery)
- **File operations**: Fixed backoff (filesystem issues)
- **Rate-limited APIs**: Linear backoff (predictable)

### 2. Set Reasonable Timeouts

- **Quick tasks** (< 1 min): 5 minute timeout
- **Medium tasks** (1-10 min): 30 minute timeout
- **Long tasks** (10-60 min): 2 hour timeout
- **Very long tasks** (> 1 hour): Consider breaking into smaller tasks

### 3. Use Trigger Rules Appropriately

- **Cleanup**: `ALL_DONE`
- **Partial results**: `ONE_SUCCESS`
- **Critical paths**: `ALL_SUCCESS` (default)
- **Error handling**: `ONE_FAILED`

### 4. Implement Idempotency

Make tasks safe to retry:

```python
def idempotent_task(**context):
    execution_date = context["execution_date"]

    # Check if already processed
    if is_already_processed(execution_date):
        logger.info("Already processed, skipping", execution_date=str(execution_date))
        return

    # Process data
    process_data(execution_date)

    # Mark as processed
    mark_as_processed(execution_date)
```

### 5. Log Comprehensively

Always log:
- Task start/end
- Retry attempts
- Error details with stack traces
- Execution context (dag_id, task_id, execution_date)
- Performance metrics (rows processed, duration)

### 6. Monitor and Alert

- **Email on failure**: Critical paths only
- **Email on retry**: Usually disable (too noisy)
- **SLA misses**: For time-sensitive pipelines
- **Callback functions**: Custom alerting logic

## Examples

### Example 1: Robust API Call

```python
from src.utils.retry_policies import create_retry_config
from src.utils.timeout_handler import create_timeout_config

retry_config = create_retry_config(
    max_retries=5,
    strategy="exponential",
    base_delay=60,
    max_delay=1800
)

timeout_config = create_timeout_config(timeout_seconds=600)

api_task = BashOperator(
    task_id="call_external_api",
    bash_command="curl -f https://api.example.com/data",
    **retry_config,
    **timeout_config,
)
```

### Example 2: Database Operation with Rollback

```python
def load_to_database(**context):
    logger.info("Starting database load")

    try:
        # Start transaction
        conn = get_connection()
        conn.begin()

        # Load data
        load_data(conn)

        # Commit
        conn.commit()
        logger.info("Database load committed")

    except Exception as e:
        # Rollback on error
        conn.rollback()
        logger.error("Database load failed, rolled back", error=str(e))
        raise

load_task = PythonOperator(
    task_id="load_database",
    python_callable=load_to_database,
    retries=2,
    retry_delay=timedelta(minutes=5),
)
```

### Example 3: Parallel Processing with Partial Failure Handling

```python
# Parallel extracts
extract_a = BashOperator(task_id="extract_a", ...)
extract_b = BashOperator(task_id="extract_b", ...)
extract_c = BashOperator(task_id="extract_c", ...)

# Merge whatever succeeded
merge = BashOperator(
    task_id="merge_results",
    bash_command="merge.sh",
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Process partial results
)

# Cleanup always runs
cleanup = BashOperator(
    task_id="cleanup",
    bash_command="cleanup.sh",
    trigger_rule=TriggerRule.ALL_DONE,
)

[extract_a, extract_b, extract_c] >> merge >> cleanup
```

## Reference

### Utility Functions

```python
# Retry policies
from src.utils.retry_policies import (
    calculate_exponential_backoff,
    calculate_linear_backoff,
    calculate_fixed_backoff,
    create_retry_config,
    generate_retry_delays,
)

# Timeout handling
from src.utils.timeout_handler import (
    TimeoutChecker,
    TimeoutContext,
    create_timeout_config,
    is_timeout_exceeded,
    format_timeout_duration,
)

# Logging
from src.utils.logger import get_logger
```

### Example DAGs

- **Beginner**: `dags/examples/beginner/demo_scheduled_pipeline_v1.py`
- **Advanced**: `dags/examples/advanced/demo_failure_recovery_v1.py`

## Next Steps

- Review example DAGs in `dags/examples/`
- Implement retry and timeout in your DAGs
- Test failure scenarios
- Monitor retry patterns in production
- Tune backoff strategies based on metrics

For more information, see:
- [Airflow Documentation - Error Handling](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#error-handling)
- [Development Guide](./development.md)
- [Testing Guide](../TESTING.md)
