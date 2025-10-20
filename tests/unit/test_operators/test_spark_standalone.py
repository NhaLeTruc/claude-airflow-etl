"""
Unit tests for Spark Standalone Operator.

Tests parameter validation, job submission, and error handling.
Following TDD approach - these tests should FAIL until implementation is complete.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

# These imports will fail until implementation exists
try:
    from src.operators.spark.standalone_operator import SparkStandaloneOperator
    from src.hooks.spark_hook import SparkJobStatus
except ImportError:
    SparkStandaloneOperator = None
    SparkJobStatus = None


@pytest.mark.unit
class TestSparkStandaloneOperator:
    """Test suite for SparkStandaloneOperator."""

    @pytest.fixture
    def mock_context(self):
        """Mock Airflow task context."""
        return {
            "task_instance": Mock(task_id="test_task"),
            "dag": Mock(dag_id="test_dag"),
            "execution_date": datetime(2024, 1, 1),
            "run_id": "manual_test_run",
        }

    def test_operator_initialization_minimal(self):
        """Test operator initialization with minimal required parameters."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
        )

        assert op.task_id == "spark_job"
        assert op.application == "/path/to/app.py"
        assert op.master == "spark://localhost:7077"

    def test_operator_initialization_full(self):
        """Test operator initialization with all parameters."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://spark-master:7077",
            application_args=["--input", "/data/input"],
            conf={
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
            },
            name="MySparkJob",
            deploy_mode="client",
            driver_memory="1g",
            driver_cores="1",
            executor_memory="2g",
            executor_cores="2",
            num_executors="3",
            verbose=True,
        )

        assert op.application_args == ["--input", "/data/input"]
        assert op.conf["spark.executor.memory"] == "2g"
        assert op.name == "MySparkJob"
        assert op.deploy_mode == "client"

    def test_missing_required_parameters(self):
        """Test that operator fails without required parameters."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        with pytest.raises(TypeError):
            # Missing application parameter
            SparkStandaloneOperator(task_id="spark_job", master="spark://localhost:7077")

    def test_invalid_master_url(self):
        """Test validation of master URL format."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        with pytest.raises(ValueError):
            SparkStandaloneOperator(
                task_id="spark_job",
                application="/path/to/app.py",
                master="invalid://url",  # Should reject non-spark:// URL
            )

    def test_invalid_deploy_mode(self):
        """Test validation of deploy mode."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        with pytest.raises(ValueError):
            SparkStandaloneOperator(
                task_id="spark_job",
                application="/path/to/app.py",
                master="spark://localhost:7077",
                deploy_mode="invalid",  # Should be 'client' or 'cluster'
            )

    def test_application_path_validation(self):
        """Test that application path is validated."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        # Should accept .py, .jar files
        op_py = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
        )
        assert op_py.application.endswith(".py")

        op_jar = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.jar",
            master="spark://localhost:7077",
        )
        assert op_jar.application.endswith(".jar")

    @patch("src.operators.spark.standalone_operator.SparkHook")
    def test_execute_success(self, mock_hook_class, mock_context):
        """Test successful job execution."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "app-12345-0001"
        mock_hook.wait_for_completion.return_value = True
        mock_hook.get_job_status.return_value = (
            SparkJobStatus.SUCCEEDED if SparkJobStatus else "SUCCEEDED"
        )
        mock_hook_class.return_value = mock_hook

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
        )

        result = op.execute(mock_context)

        mock_hook.submit_job.assert_called_once()
        mock_hook.wait_for_completion.assert_called_once()
        assert result is not None

    @patch("src.operators.spark.standalone_operator.SparkHook")
    def test_execute_with_application_args(self, mock_hook_class, mock_context):
        """Test execution with application arguments."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "app-12345-0001"
        mock_hook.wait_for_completion.return_value = True
        mock_hook_class.return_value = mock_hook

        app_args = ["--input", "/data/input", "--output", "/data/output"]

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
            application_args=app_args,
        )

        op.execute(mock_context)

        # Verify application_args were passed to hook
        call_kwargs = mock_hook.submit_job.call_args[1]
        assert call_kwargs.get("application_args") == app_args

    @patch("src.operators.spark.standalone_operator.SparkHook")
    def test_execute_with_spark_conf(self, mock_hook_class, mock_context):
        """Test execution with Spark configuration."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "app-12345-0001"
        mock_hook.wait_for_completion.return_value = True
        mock_hook_class.return_value = mock_hook

        conf = {
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
        }

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
            conf=conf,
        )

        op.execute(mock_context)

        call_kwargs = mock_hook.submit_job.call_args[1]
        assert call_kwargs.get("conf") == conf

    @patch("src.operators.spark.standalone_operator.SparkHook")
    def test_execute_failure(self, mock_hook_class, mock_context):
        """Test job execution failure handling."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.return_value = "app-12345-0001"
        mock_hook.wait_for_completion.return_value = False
        mock_hook.get_job_status.return_value = (
            SparkJobStatus.FAILED if SparkJobStatus else "FAILED"
        )
        mock_hook_class.return_value = mock_hook

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
        )

        with pytest.raises(Exception):  # Should raise AirflowException
            op.execute(mock_context)

    @patch("src.operators.spark.standalone_operator.SparkHook")
    def test_execute_submission_error(self, mock_hook_class, mock_context):
        """Test handling of job submission errors."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        mock_hook = Mock()
        mock_hook.submit_job.side_effect = Exception("spark-submit not found")
        mock_hook_class.return_value = mock_hook

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
        )

        with pytest.raises(Exception):
            op.execute(mock_context)

    @patch("src.operators.spark.standalone_operator.SparkHook")
    def test_on_kill(self, mock_hook_class):
        """Test task kill handling."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        mock_hook = Mock()
        mock_hook_class.return_value = mock_hook

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
        )

        # Simulate job running
        op._job_id = "app-12345-0001"

        op.on_kill()

        # Verify kill was called
        mock_hook.kill_job.assert_called_once_with("app-12345-0001")

    def test_template_fields(self):
        """Test that operator has correct template fields."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        # These fields should be templatable (Jinja2)
        assert "application" in SparkStandaloneOperator.template_fields
        assert "application_args" in SparkStandaloneOperator.template_fields

    def test_ui_color(self):
        """Test that operator has a distinct UI color."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
        )

        assert hasattr(op, "ui_color")
        assert op.ui_color is not None

    def test_resource_configuration(self):
        """Test that resource configurations are properly set."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
            driver_memory="2g",
            driver_cores="2",
            executor_memory="4g",
            executor_cores="4",
            num_executors="5",
        )

        assert op.driver_memory == "2g"
        assert op.driver_cores == "2"
        assert op.executor_memory == "4g"
        assert op.executor_cores == "4"
        assert op.num_executors == "5"

    def test_verbose_mode(self):
        """Test that verbose mode is configurable."""
        if SparkStandaloneOperator is None:
            pytest.skip("SparkStandaloneOperator not yet implemented")

        op = SparkStandaloneOperator(
            task_id="spark_job",
            application="/path/to/app.py",
            master="spark://localhost:7077",
            verbose=True,
        )

        assert op.verbose is True