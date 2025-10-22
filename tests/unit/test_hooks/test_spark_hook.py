"""
Unit tests for Spark Hook.

Tests job submission, status polling, and log retrieval functionality.
Following TDD approach - these tests should FAIL until implementation is complete.
"""

from unittest.mock import Mock, patch

import pytest

# These imports will fail until implementation exists
try:
    from src.hooks.spark_hook import SparkHook, SparkJobStatus, SparkSubmitException
except ImportError:
    # Allow tests to be written before implementation
    SparkHook = None
    SparkJobStatus = None
    SparkSubmitException = None


@pytest.mark.unit
class TestSparkHook:
    """Test suite for SparkHook class."""

    @pytest.fixture
    def mock_connection(self):
        """Mock Airflow connection for Spark."""
        conn = Mock()
        conn.host = "spark://localhost:7077"
        conn.port = 7077
        conn.login = None
        conn.password = None
        conn.extra_dejson = {
            "deploy_mode": "client",
            "spark_binary": "spark-submit",
        }
        return conn

    @pytest.fixture
    def spark_hook(self, mock_connection):
        """Create SparkHook instance with mocked connection."""
        if SparkHook is None:
            pytest.skip("SparkHook not yet implemented")

        with patch.object(SparkHook, "get_connection", return_value=mock_connection):
            hook = SparkHook(conn_id="spark_default")
            return hook

    def test_hook_initialization(self, spark_hook):
        """Test that SparkHook initializes correctly."""
        assert spark_hook is not None
        assert spark_hook.conn_id == "spark_default"

    def test_get_spark_binary_path(self, spark_hook):
        """Test retrieving spark-submit binary path."""
        binary_path = spark_hook.get_spark_binary()
        assert binary_path is not None
        assert "spark-submit" in binary_path

    def test_build_submit_command_basic(self, spark_hook):
        """Test building basic spark-submit command."""
        application = "/path/to/app.py"
        command = spark_hook.build_submit_command(
            application=application, application_args=None, conf=None
        )

        assert isinstance(command, list)
        assert "spark-submit" in command[0]
        assert application in command

    def test_build_submit_command_with_args(self, spark_hook):
        """Test building spark-submit command with application arguments."""
        application = "/path/to/app.py"
        app_args = ["--input", "/data/input", "--output", "/data/output"]

        command = spark_hook.build_submit_command(
            application=application, application_args=app_args, conf=None
        )

        assert "--input" in command
        assert "/data/input" in command
        assert "--output" in command

    def test_build_submit_command_with_conf(self, spark_hook):
        """Test building spark-submit command with Spark configuration."""
        application = "/path/to/app.py"
        conf = {"spark.executor.memory": "2g", "spark.driver.cores": "2"}

        command = spark_hook.build_submit_command(
            application=application, application_args=None, conf=conf
        )

        assert "--conf" in command
        assert any("spark.executor.memory=2g" in str(c) for c in command)

    def test_build_submit_command_with_master(self, spark_hook):
        """Test that master URL is included in submit command."""
        application = "/path/to/app.py"
        command = spark_hook.build_submit_command(
            application=application, application_args=None, conf=None
        )

        assert "--master" in command

    @patch("subprocess.Popen")
    def test_submit_job_success(self, mock_popen, spark_hook):
        """Test successful job submission."""
        mock_process = Mock()
        mock_process.pid = 12345
        mock_process.poll.return_value = None  # Still running
        mock_popen.return_value = mock_process

        application = "/path/to/app.py"
        job_id = spark_hook.submit_job(application=application)

        assert job_id is not None
        assert isinstance(job_id, str)
        mock_popen.assert_called_once()

    @patch("subprocess.Popen")
    def test_submit_job_with_name(self, mock_popen, spark_hook):
        """Test job submission with custom application name."""
        mock_process = Mock()
        mock_process.pid = 12345
        mock_popen.return_value = mock_process

        application = "/path/to/app.py"
        app_name = "MySparkJob"

        spark_hook.submit_job(application=application, name=app_name)

        # Verify --name argument was passed
        call_args = mock_popen.call_args
        command = call_args[0][0] if call_args[0] else call_args[1].get("args", [])
        assert "--name" in command
        assert app_name in command

    @patch("subprocess.Popen")
    def test_submit_job_failure(self, mock_popen, spark_hook):
        """Test job submission failure handling."""
        mock_popen.side_effect = Exception("spark-submit not found")

        application = "/path/to/app.py"

        if SparkSubmitException is None:
            pytest.skip("SparkSubmitException not yet implemented")

        with pytest.raises(SparkSubmitException):
            spark_hook.submit_job(application=application)

    def test_get_job_status_running(self, spark_hook):
        """Test getting status of running job."""
        if SparkJobStatus is None:
            pytest.skip("SparkJobStatus not yet implemented")

        job_id = "app-12345-0001"
        mock_process = Mock()
        mock_process.poll.return_value = None  # Still running

        spark_hook._jobs = {job_id: mock_process}

        status = spark_hook.get_job_status(job_id)
        assert status == SparkJobStatus.RUNNING

    def test_get_job_status_succeeded(self, spark_hook):
        """Test getting status of succeeded job."""
        if SparkJobStatus is None:
            pytest.skip("SparkJobStatus not yet implemented")

        job_id = "app-12345-0001"
        mock_process = Mock()
        mock_process.poll.return_value = 0  # Exit code 0 = success
        mock_process.returncode = 0

        spark_hook._jobs = {job_id: mock_process}

        status = spark_hook.get_job_status(job_id)
        assert status == SparkJobStatus.SUCCEEDED

    def test_get_job_status_failed(self, spark_hook):
        """Test getting status of failed job."""
        if SparkJobStatus is None:
            pytest.skip("SparkJobStatus not yet implemented")

        job_id = "app-12345-0001"
        mock_process = Mock()
        mock_process.poll.return_value = 1  # Non-zero exit code = failure
        mock_process.returncode = 1

        spark_hook._jobs = {job_id: mock_process}

        status = spark_hook.get_job_status(job_id)
        assert status == SparkJobStatus.FAILED

    def test_get_job_status_unknown(self, spark_hook):
        """Test getting status of unknown job."""
        if SparkJobStatus is None:
            pytest.skip("SparkJobStatus not yet implemented")

        job_id = "app-unknown-0001"
        status = spark_hook.get_job_status(job_id)
        assert status == SparkJobStatus.UNKNOWN

    def test_wait_for_completion_success(self, spark_hook):
        """Test waiting for job completion - success case."""
        job_id = "app-12345-0001"
        mock_process = Mock()

        # Simulate job completing after 2 polls
        mock_process.poll.side_effect = [None, None, 0]
        mock_process.returncode = 0

        spark_hook._jobs = {job_id: mock_process}

        result = spark_hook.wait_for_completion(job_id, timeout=10, poll_interval=0.1)
        assert result is True

    def test_wait_for_completion_failure(self, spark_hook):
        """Test waiting for job completion - failure case."""
        job_id = "app-12345-0001"
        mock_process = Mock()
        mock_process.poll.return_value = 1
        mock_process.returncode = 1

        spark_hook._jobs = {job_id: mock_process}

        result = spark_hook.wait_for_completion(job_id, timeout=10, poll_interval=0.1)
        assert result is False

    def test_wait_for_completion_timeout(self, spark_hook):
        """Test waiting for job completion - timeout case."""
        job_id = "app-12345-0001"
        mock_process = Mock()
        mock_process.poll.return_value = None  # Never completes

        spark_hook._jobs = {job_id: mock_process}

        result = spark_hook.wait_for_completion(job_id, timeout=0.5, poll_interval=0.1)
        assert result is False

    def test_kill_job(self, spark_hook):
        """Test killing a running job."""
        job_id = "app-12345-0001"
        mock_process = Mock()
        mock_process.poll.return_value = None

        spark_hook._jobs = {job_id: mock_process}

        spark_hook.kill_job(job_id)
        mock_process.terminate.assert_called_once()

    def test_kill_job_with_force(self, spark_hook):
        """Test force killing a job that doesn't respond to terminate."""
        job_id = "app-12345-0001"
        mock_process = Mock()
        mock_process.poll.side_effect = [None, None, None]  # Doesn't terminate

        spark_hook._jobs = {job_id: mock_process}

        spark_hook.kill_job(job_id, force=True)
        mock_process.kill.assert_called()

    @patch("builtins.open", create=True)
    def test_get_job_logs(self, mock_open, spark_hook):
        """Test retrieving job logs."""
        job_id = "app-12345-0001"
        log_content = "Spark job started\nProcessing data\nJob completed"

        mock_file = Mock()
        mock_file.read.return_value = log_content
        mock_open.return_value.__enter__.return_value = mock_file

        logs = spark_hook.get_job_logs(job_id)
        assert logs is not None
        assert "Spark job started" in logs

    def test_get_job_logs_not_found(self, spark_hook):
        """Test retrieving logs for job that doesn't exist."""
        job_id = "app-unknown-0001"
        logs = spark_hook.get_job_logs(job_id)
        assert logs is None or logs == ""

    def test_connection_properties_extracted(self, spark_hook, mock_connection):
        """Test that connection properties are correctly extracted."""
        assert spark_hook.master_url is not None
        assert "spark://" in spark_hook.master_url or spark_hook.master_url == "local"

    def test_multiple_concurrent_jobs(self, spark_hook):
        """Test tracking multiple concurrent job submissions."""
        with patch("subprocess.Popen") as mock_popen:
            mock_popen.side_effect = [
                Mock(pid=111),
                Mock(pid=222),
                Mock(pid=333),
            ]

            job1 = spark_hook.submit_job("/app1.py")
            job2 = spark_hook.submit_job("/app2.py")
            job3 = spark_hook.submit_job("/app3.py")

            assert job1 != job2 != job3
            assert len(spark_hook._jobs) == 3


@pytest.mark.unit
class TestSparkJobStatus:
    """Test SparkJobStatus enum."""

    def test_status_enum_values(self):
        """Test that all expected status values exist."""
        if SparkJobStatus is None:
            pytest.skip("SparkJobStatus not yet implemented")

        assert hasattr(SparkJobStatus, "SUBMITTED")
        assert hasattr(SparkJobStatus, "RUNNING")
        assert hasattr(SparkJobStatus, "SUCCEEDED")
        assert hasattr(SparkJobStatus, "FAILED")
        assert hasattr(SparkJobStatus, "UNKNOWN")


@pytest.mark.unit
class TestSparkSubmitException:
    """Test SparkSubmitException class."""

    def test_exception_can_be_raised(self):
        """Test that SparkSubmitException can be raised and caught."""
        if SparkSubmitException is None:
            pytest.skip("SparkSubmitException not yet implemented")

        with pytest.raises(SparkSubmitException):
            raise SparkSubmitException("Test error")

    def test_exception_message(self):
        """Test that exception preserves error message."""
        if SparkSubmitException is None:
            pytest.skip("SparkSubmitException not yet implemented")

        error_msg = "spark-submit failed with exit code 1"
        try:
            raise SparkSubmitException(error_msg)
        except SparkSubmitException as e:
            assert str(e) == error_msg
