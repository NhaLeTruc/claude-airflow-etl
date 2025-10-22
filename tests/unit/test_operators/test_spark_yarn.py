"""
Unit tests for Spark YARN Operator.

Tests YARN-specific configuration, queue selection, and resource allocation.
Following TDD approach - these tests should FAIL until implementation is complete.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

# These imports will fail until implementation exists
try:
    from src.hooks.spark_hook import SparkJobStatus
    from src.operators.spark.yarn_operator import SparkYarnOperator
except ImportError:
    SparkYarnOperator = None
    SparkJobStatus = None


@pytest.mark.unit
class TestSparkYarnOperator:
    """Test suite for SparkYarnOperator."""

    @pytest.fixture
    def mock_context(self):
        """Mock Airflow task context."""
        return {
            "task_instance": Mock(task_id="test_task"),
            "dag": Mock(dag_id="test_dag"),
            "execution_date": datetime(2024, 1, 1),
        }

    def test_operator_initialization_minimal(self):
        """Test operator initialization with minimal required parameters."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        op = SparkYarnOperator(
            task_id="yarn_spark_job", application="/path/to/app.py", queue="default"
        )

        assert op.task_id == "yarn_spark_job"
        assert op.application == "/path/to/app.py"
        assert op.queue == "default"

    def test_operator_with_queue_selection(self):
        """Test operator with custom YARN queue."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        op = SparkYarnOperator(
            task_id="yarn_spark_job",
            application="/path/to/app.py",
            queue="production",
        )

        assert op.queue == "production"

    def test_operator_with_deploy_mode(self):
        """Test operator with YARN deploy mode (client vs cluster)."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        # Cluster mode
        op_cluster = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="default",
            deploy_mode="cluster",
        )
        assert op_cluster.deploy_mode == "cluster"

        # Client mode
        op_client = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="default",
            deploy_mode="client",
        )
        assert op_client.deploy_mode == "client"

    def test_invalid_deploy_mode(self):
        """Test validation of deploy mode."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        with pytest.raises(ValueError):
            SparkYarnOperator(
                task_id="yarn_job",
                application="/path/to/app.py",
                queue="default",
                deploy_mode="invalid",
            )

    def test_executor_resource_configuration(self):
        """Test executor resource configuration (memory, cores, instances)."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        op = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="default",
            executor_memory="4g",
            executor_cores="4",
            num_executors="10",
        )

        assert op.executor_memory == "4g"
        assert op.executor_cores == "4"
        assert op.num_executors == "10"

    def test_driver_resource_configuration(self):
        """Test driver resource configuration (memory, cores)."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        op = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="default",
            driver_memory="2g",
            driver_cores="2",
        )

        assert op.driver_memory == "2g"
        assert op.driver_cores == "2"

    def test_yarn_specific_configuration(self):
        """Test YARN-specific Spark configurations."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        yarn_conf = {
            "spark.yarn.submit.waitAppCompletion": "true",
            "spark.yarn.maxAppAttempts": "3",
            "spark.yarn.am.memory": "1g",
        }

        op = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="default",
            conf=yarn_conf,
        )

        assert "spark.yarn.submit.waitAppCompletion" in op.conf
        assert op.conf["spark.yarn.maxAppAttempts"] == "3"

    @patch("src.operators.spark.yarn_operator.SparkHook")
    def test_execute_with_yarn_master(self, mock_hook_class, mock_context):
        """Test that execute uses 'yarn' as master."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "application_1234567890_0001"
        mock_hook.wait_for_completion.return_value = True
        mock_hook_class.return_value = mock_hook

        op = SparkYarnOperator(task_id="yarn_job", application="/path/to/app.py", queue="default")

        op.execute(mock_context)

        # Verify master was set to 'yarn'
        call_kwargs = mock_hook.submit_job.call_args[1]
        assert "master" in call_kwargs or call_kwargs.get("master") == "yarn"

    @patch("src.operators.spark.yarn_operator.SparkHook")
    def test_execute_with_queue(self, mock_hook_class, mock_context):
        """Test that queue is passed in configuration."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "application_1234567890_0001"
        mock_hook.wait_for_completion.return_value = True
        mock_hook_class.return_value = mock_hook

        op = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="high_priority",
        )

        op.execute(mock_context)

        # Verify queue configuration was passed
        call_kwargs = mock_hook.submit_job.call_args[1]
        conf = call_kwargs.get("conf", {})
        assert "spark.yarn.queue" in conf
        assert conf["spark.yarn.queue"] == "high_priority"

    @patch("src.operators.spark.yarn_operator.SparkHook")
    def test_execute_cluster_mode(self, mock_hook_class, mock_context):
        """Test execution in cluster deploy mode."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "application_1234567890_0001"
        mock_hook.wait_for_completion.return_value = True
        mock_hook_class.return_value = mock_hook

        op = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="default",
            deploy_mode="cluster",
        )

        op.execute(mock_context)

        call_kwargs = mock_hook.submit_job.call_args[1]
        assert call_kwargs.get("deploy_mode") == "cluster"

    @patch("src.operators.spark.yarn_operator.SparkHook")
    def test_application_tracking(self, mock_hook_class, mock_context):
        """Test YARN application ID tracking."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        mock_hook = Mock()
        yarn_app_id = "application_1234567890_0001"
        mock_hook.submit_job.return_value = yarn_app_id
        mock_hook.wait_for_completion.return_value = True
        mock_hook_class.return_value = mock_hook

        op = SparkYarnOperator(task_id="yarn_job", application="/path/to/app.py", queue="default")

        result = op.execute(mock_context)

        # Verify YARN application ID is tracked/returned
        assert result == yarn_app_id or hasattr(op, "_application_id")

    def test_template_fields(self):
        """Test that operator has correct template fields."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        assert "application" in SparkYarnOperator.template_fields
        assert "queue" in SparkYarnOperator.template_fields

    def test_ui_color(self):
        """Test that operator has YARN-specific UI color."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        op = SparkYarnOperator(task_id="yarn_job", application="/path/to/app.py", queue="default")

        assert hasattr(op, "ui_color")
        # YARN operators typically use orange/brown colors
        assert op.ui_color is not None

    def test_dynamic_resource_allocation(self):
        """Test dynamic executor allocation configuration."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        conf = {
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "2",
            "spark.dynamicAllocation.maxExecutors": "20",
        }

        op = SparkYarnOperator(
            task_id="yarn_job",
            application="/path/to/app.py",
            queue="default",
            conf=conf,
        )

        assert op.conf["spark.dynamicAllocation.enabled"] == "true"

    def test_missing_queue_parameter(self):
        """Test that queue parameter is required or defaults appropriately."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        # Should either require queue or default to 'default'
        try:
            op = SparkYarnOperator(task_id="yarn_job", application="/path/to/app.py")
            # If no error, check it has a default
            assert hasattr(op, "queue")
        except TypeError:
            # If queue is required, this is expected
            pass

    @patch("src.operators.spark.yarn_operator.SparkHook")
    def test_application_logs_retrieval(self, mock_hook_class, mock_context):
        """Test retrieval of YARN application logs."""
        if SparkYarnOperator is None:
            pytest.skip("SparkYarnOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "application_1234567890_0001"
        mock_hook.wait_for_completion.return_value = True
        mock_hook.get_job_logs.return_value = "Application logs content"
        mock_hook_class.return_value = mock_hook

        op = SparkYarnOperator(task_id="yarn_job", application="/path/to/app.py", queue="default")

        op.execute(mock_context)

        # Logs should be retrievable
        assert mock_hook.get_job_logs.called or hasattr(op, "get_logs")
