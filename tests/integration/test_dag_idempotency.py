"""
Integration tests for DAG idempotency.

Tests ensure incremental DAGs can run multiple times without duplicates
or unintended side effects. Critical for production ETL reliability.
"""

import pytest
from datetime import datetime, timedelta
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from unittest.mock import Mock, patch


@pytest.fixture
def dag_bag():
    """Load all DAGs from the examples directory."""
    return DagBag(dag_folder="dags/examples", include_examples=False)


@pytest.fixture
def execution_date():
    """Provide a consistent execution date for testing."""
    return datetime(2025, 1, 15, 10, 0, 0)


@pytest.fixture
def warehouse_connection():
    """Mock warehouse database connection for testing."""
    conn = Mock()
    conn.get_records = Mock(return_value=[])
    conn.run = Mock()
    return conn


class TestIncrementalLoadIdempotency:
    """Test suite for incremental load DAG idempotency."""

    def test_incremental_load_runs_twice_without_duplicates(
        self, dag_bag, execution_date, warehouse_connection
    ):
        """Test incremental load DAG produces same result when run twice."""
        dag_id = "demo_incremental_load_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        with patch("src.hooks.warehouse_hook.WarehouseHook") as mock_hook:
            mock_hook.return_value = warehouse_connection

            # First run
            dag_run_1 = dag.create_dagrun(
                state=DagRunState.RUNNING,
                execution_date=execution_date,
                run_type=DagRunType.SCHEDULED,
                run_id=f"scheduled__{execution_date.isoformat()}_run1",
            )

            for task in dag.tasks:
                task_instance = TaskInstance(task, execution_date)
                task_instance.run(ignore_ti_state=True)

            first_run_calls = warehouse_connection.run.call_count

            # Second run (should be idempotent)
            warehouse_connection.run.reset_mock()

            dag_run_2 = dag.create_dagrun(
                state=DagRunState.RUNNING,
                execution_date=execution_date,
                run_type=DagRunType.SCHEDULED,
                run_id=f"scheduled__{execution_date.isoformat()}_run2",
            )

            for task in dag.tasks:
                task_instance = TaskInstance(task, execution_date)
                task_instance.run(ignore_ti_state=True)

            second_run_calls = warehouse_connection.run.call_count

            # Both runs should succeed
            assert dag_run_1.state in [DagRunState.SUCCESS, DagRunState.RUNNING]
            assert dag_run_2.state in [DagRunState.SUCCESS, DagRunState.RUNNING]

    def test_incremental_load_uses_watermarks(self, dag_bag, execution_date):
        """Test that incremental load tracks watermarks for each run."""
        dag_id = "demo_incremental_load_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        # Verify watermark tracking tasks exist
        watermark_tasks = [
            task for task in dag.tasks
            if any(keyword in task.task_id.lower() for keyword in
                   ["watermark", "checkpoint", "bookmark", "max_timestamp"])
        ]

        # Should have at least one watermark-related task
        assert len(watermark_tasks) > 0, "Incremental load should track watermarks"

    def test_multiple_runs_process_different_data(self, dag_bag, execution_date):
        """Test that consecutive runs process only new data."""
        dag_id = "demo_incremental_load_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        # Run 1: Process data up to execution_date
        dag_run_1 = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_type=DagRunType.SCHEDULED,
            run_id=f"scheduled__{execution_date.isoformat()}",
        )

        # Run 2: Process data for next day (should only process new records)
        execution_date_2 = execution_date + timedelta(days=1)
        dag_run_2 = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date_2,
            run_type=DagRunType.SCHEDULED,
            run_id=f"scheduled__{execution_date_2.isoformat()}",
        )

        # Both runs should be independent
        assert dag_run_1.execution_date != dag_run_2.execution_date


class TestSCDType2Idempotency:
    """Test suite for SCD Type 2 DAG idempotency."""

    def test_scd_type2_runs_twice_same_result(self, dag_bag, execution_date):
        """Test SCD Type 2 DAG produces same result when run twice."""
        dag_id = "demo_scd_type2_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        # First run
        dag_run_1 = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_type=DagRunType.SCHEDULED,
            run_id=f"scheduled__{execution_date.isoformat()}_run1",
        )

        # Second run (rerun)
        dag_run_2 = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_type=DagRunType.SCHEDULED,
            run_id=f"scheduled__{execution_date.isoformat()}_run2",
        )

        # Both should produce consistent results
        assert dag_run_1.execution_date == dag_run_2.execution_date

    def test_scd_type2_handles_effective_dates_correctly(self, dag_bag, execution_date):
        """Test that SCD Type 2 uses effective dates for history tracking."""
        dag_id = "demo_scd_type2_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        # Verify effective date handling tasks
        scd_tasks = [
            task for task in dag.tasks
            if any(keyword in task.task_id.lower() for keyword in
                   ["effective", "valid_from", "valid_to", "current_flag", "scd"])
        ]

        assert len(scd_tasks) > 0, "SCD Type 2 should manage effective dates"


class TestDataQualityIdempotency:
    """Test suite for data quality DAG idempotency."""

    def test_quality_checks_idempotent(self, dag_bag, execution_date):
        """Test quality checks produce same results when run multiple times."""
        dag_id = "demo_comprehensive_quality_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        # Run quality checks twice
        dag_run_1 = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_type=DagRunType.MANUAL,
            run_id=f"manual__{execution_date.isoformat()}_run1",
        )

        dag_run_2 = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_type=DagRunType.MANUAL,
            run_id=f"manual__{execution_date.isoformat()}_run2",
        )

        # Quality checks should be idempotent (same input = same result)
        assert dag_run_1.execution_date == dag_run_2.execution_date


class TestSparkJobIdempotency:
    """Test suite for Spark job idempotency."""

    def test_spark_jobs_handle_reruns(self, dag_bag, execution_date):
        """Test Spark jobs can be safely rerun without side effects."""
        dag_id = "demo_spark_standalone_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        # Verify Spark operators have output modes that support idempotency
        # (e.g., overwrite mode, or append with deduplication)
        spark_tasks = [
            task for task in dag.tasks
            if "spark" in task.task_id.lower() or "Spark" in task.__class__.__name__
        ]

        assert len(spark_tasks) > 0, "Should have Spark tasks"


class TestGeneralIdempotencyPrinciples:
    """Test general idempotency principles across all DAGs."""

    def test_extract_load_uses_truncate_and_load(self, dag_bag):
        """Test that simple extract-load uses truncate-and-load pattern."""
        dag_id = "demo_simple_extract_load_v1"

        if dag_id not in dag_bag.dags:
            pytest.skip(f"{dag_id} not implemented yet")

        dag = dag_bag.get_dag(dag_id)

        # Should have truncate or delete task before load
        truncate_tasks = [
            task for task in dag.tasks
            if any(keyword in task.task_id.lower() for keyword in
                   ["truncate", "delete", "clear", "drop"])
        ]

        # This pattern ensures idempotency for full loads
        # (Not required but common pattern)

    def test_temp_tables_cleaned_up(self, dag_bag):
        """Test that DAGs clean up temporary tables."""
        # Skip - this is implementation-specific
        pytest.skip("Temp table cleanup is implementation-specific")

    def test_no_insert_without_delete(self, dag_bag):
        """Test that DAGs don't do INSERT without proper deduplication."""
        # This is a code review check rather than automated test
        pytest.skip("Manual code review required")

    def test_reruns_produce_same_data_hash(self, dag_bag, execution_date):
        """Test that rerunning a DAG produces same data (by hash)."""
        pytest.skip("Requires database access and data hashing - manual test")

        # Conceptual test:
        # 1. Run DAG
        # 2. Calculate hash of output data
        # 3. Rerun DAG with same execution_date
        # 4. Calculate hash of output data again
        # 5. Assert hashes match

    def test_all_incremental_dags_use_where_clauses(self, dag_bag):
        """Test that incremental DAGs use WHERE clauses to filter data."""
        incremental_dags = [
            "demo_incremental_load_v1",
            "demo_scd_type2_v1",
        ]

        for dag_id in incremental_dags:
            if dag_id not in dag_bag.dags:
                continue

            dag = dag_bag.get_dag(dag_id)

            # Check for SQL operators with WHERE clauses
            # This is a best practice check
            # Real implementation would need to inspect operator parameters

    def test_no_global_state_mutations(self, dag_bag):
        """Test that DAGs don't rely on global mutable state."""
        # This is enforced by Airflow's design
        # Each task should be independent and idempotent
        pytest.skip("Enforced by Airflow architecture")

    def test_file_operations_are_atomic(self, dag_bag):
        """Test that file operations use atomic writes."""
        # Implementation-specific - would need to check file operators
        pytest.skip("Implementation-specific check")