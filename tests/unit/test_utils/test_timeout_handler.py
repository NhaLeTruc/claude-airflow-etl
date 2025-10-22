"""
Unit tests for timeout handling utility.

Tests task termination on timeout and correct status marking.
"""

from datetime import datetime, timedelta

import pytest


class TestTimeoutHandler:
    """Test suite for timeout handling utility."""

    def test_timeout_checker_detects_timeout(self):
        """Test that timeout is detected when time exceeds limit."""
        from src.utils.timeout_handler import TimeoutChecker

        checker = TimeoutChecker(timeout_seconds=300)  # 5 minute timeout

        # Start time
        start_time = datetime(2024, 1, 1, 12, 0, 0)

        # 6 minutes later (exceeded timeout)
        current_time = datetime(2024, 1, 1, 12, 6, 0)

        is_timeout = checker.check_timeout(start_time, current_time)
        assert is_timeout is True

    def test_timeout_checker_within_limit(self):
        """Test that no timeout when within time limit."""
        from src.utils.timeout_handler import TimeoutChecker

        checker = TimeoutChecker(timeout_seconds=300)

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = datetime(2024, 1, 1, 12, 4, 0)  # 4 minutes (within 5 minute limit)

        is_timeout = checker.check_timeout(start_time, current_time)
        assert is_timeout is False

    def test_timeout_checker_exactly_at_limit(self):
        """Test behavior at exact timeout threshold."""
        from src.utils.timeout_handler import TimeoutChecker

        checker = TimeoutChecker(timeout_seconds=300)

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = datetime(2024, 1, 1, 12, 5, 0)  # Exactly 5 minutes

        # Exact match should not timeout (use > not >=)
        is_timeout = checker.check_timeout(start_time, current_time)
        assert is_timeout is False

    def test_calculate_elapsed_time(self):
        """Test calculation of elapsed time."""
        from src.utils.timeout_handler import calculate_elapsed_time

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        end_time = datetime(2024, 1, 1, 12, 10, 30)

        elapsed = calculate_elapsed_time(start_time, end_time)

        assert elapsed == 630  # 10 minutes 30 seconds = 630 seconds

    def test_calculate_elapsed_time_as_timedelta(self):
        """Test elapsed time returns timedelta."""
        from src.utils.timeout_handler import calculate_elapsed_time

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        end_time = datetime(2024, 1, 1, 12, 5, 0)

        elapsed = calculate_elapsed_time(start_time, end_time, return_timedelta=True)

        assert isinstance(elapsed, timedelta)
        assert elapsed.total_seconds() == 300

    def test_timeout_remaining_calculation(self):
        """Test calculation of remaining time before timeout."""
        from src.utils.timeout_handler import calculate_timeout_remaining

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = datetime(2024, 1, 1, 12, 3, 0)  # 3 minutes elapsed
        timeout_seconds = 300  # 5 minute timeout

        remaining = calculate_timeout_remaining(start_time, current_time, timeout_seconds)

        assert remaining == 120  # 2 minutes (120 seconds) remaining

    def test_timeout_remaining_when_exceeded(self):
        """Test remaining time is negative when timeout exceeded."""
        from src.utils.timeout_handler import calculate_timeout_remaining

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = datetime(2024, 1, 1, 12, 7, 0)  # 7 minutes elapsed
        timeout_seconds = 300  # 5 minute timeout

        remaining = calculate_timeout_remaining(start_time, current_time, timeout_seconds)

        assert remaining == -120  # 2 minutes over limit (negative)

    def test_is_timeout_exceeded_function(self):
        """Test simple timeout exceeded check."""
        from src.utils.timeout_handler import is_timeout_exceeded

        start_time = datetime(2024, 1, 1, 12, 0, 0)

        # Not exceeded
        current_time = datetime(2024, 1, 1, 12, 4, 0)
        assert is_timeout_exceeded(start_time, current_time, timeout_seconds=300) is False

        # Exceeded
        current_time = datetime(2024, 1, 1, 12, 6, 0)
        assert is_timeout_exceeded(start_time, current_time, timeout_seconds=300) is True

    def test_timeout_context_manager(self):
        """Test timeout context manager for task execution."""
        from src.utils.timeout_handler import TimeoutContext

        executed = []

        def task_function():
            executed.append("task")
            return "success"

        with TimeoutContext(timeout_seconds=5) as ctx:
            result = task_function()

        assert result == "success"
        assert executed == ["task"]
        assert ctx.timed_out is False

    def test_timeout_context_marks_timeout(self):
        """Test that context manager marks timeout when exceeded."""

        from src.utils.timeout_handler import TimeoutContext

        with TimeoutContext(timeout_seconds=1) as ctx:
            # Mock sleep to simulate long execution
            ctx.start_time = datetime.now() - timedelta(seconds=2)

        # After context, check if timeout was detected
        assert ctx.is_timed_out() is True

    def test_timeout_configuration_for_airflow_task(self):
        """Test creating timeout config for Airflow task."""
        from src.utils.timeout_handler import create_timeout_config

        config = create_timeout_config(timeout_seconds=1800)  # 30 minutes

        assert "execution_timeout" in config
        assert isinstance(config["execution_timeout"], timedelta)
        assert config["execution_timeout"].total_seconds() == 1800

    def test_timeout_config_with_callable(self):
        """Test timeout config with on_timeout callback."""
        from src.utils.timeout_handler import create_timeout_config

        callback_called = []

        def on_timeout_callback(context):
            callback_called.append(context)

        config = create_timeout_config(timeout_seconds=600, on_timeout_callback=on_timeout_callback)

        assert "execution_timeout" in config
        assert "on_failure_callback" in config

    def test_format_timeout_duration(self):
        """Test formatting timeout duration for display."""
        from src.utils.timeout_handler import format_timeout_duration

        # Seconds
        assert format_timeout_duration(45) == "45 seconds"
        assert format_timeout_duration(90) == "1 minute 30 seconds"

        # Minutes
        assert format_timeout_duration(300) == "5 minutes"
        assert format_timeout_duration(330) == "5 minutes 30 seconds"

        # Hours
        assert format_timeout_duration(3600) == "1 hour"
        assert format_timeout_duration(3661) == "1 hour 1 minute 1 second"
        assert format_timeout_duration(7200) == "2 hours"

    def test_timeout_zero_means_no_timeout(self):
        """Test that timeout of 0 means no timeout enforcement."""
        from src.utils.timeout_handler import is_timeout_exceeded

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = datetime(2024, 1, 1, 18, 0, 0)  # 6 hours later

        # timeout_seconds=0 means no timeout
        assert is_timeout_exceeded(start_time, current_time, timeout_seconds=0) is False

    def test_timeout_none_means_no_timeout(self):
        """Test that None timeout means no timeout enforcement."""
        from src.utils.timeout_handler import is_timeout_exceeded

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = datetime(2024, 1, 1, 18, 0, 0)

        assert is_timeout_exceeded(start_time, current_time, timeout_seconds=None) is False

    def test_negative_timeout_raises_error(self):
        """Test that negative timeout raises error."""
        from src.utils.timeout_handler import TimeoutChecker

        with pytest.raises(ValueError) as exc_info:
            TimeoutChecker(timeout_seconds=-100)

        assert (
            "negative" in str(exc_info.value).lower() or "positive" in str(exc_info.value).lower()
        )

    def test_timeout_warning_threshold(self):
        """Test warning when approaching timeout."""
        from src.utils.timeout_handler import should_warn_about_timeout

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        timeout_seconds = 600  # 10 minutes

        # At 8 minutes (80% of timeout) - should warn
        current_time = datetime(2024, 1, 1, 12, 8, 0)
        assert (
            should_warn_about_timeout(start_time, current_time, timeout_seconds, threshold=0.8)
            is True
        )

        # At 5 minutes (50% of timeout) - should not warn
        current_time = datetime(2024, 1, 1, 12, 5, 0)
        assert (
            should_warn_about_timeout(start_time, current_time, timeout_seconds, threshold=0.8)
            is False
        )

    def test_get_timeout_status_string(self):
        """Test getting human-readable timeout status."""
        from src.utils.timeout_handler import get_timeout_status

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = datetime(2024, 1, 1, 12, 3, 0)
        timeout_seconds = 600

        status = get_timeout_status(start_time, current_time, timeout_seconds)

        assert "remaining" in status.lower() or "elapsed" in status.lower()
        assert isinstance(status, str)

    @pytest.mark.parametrize(
        "elapsed,timeout,expected",
        [
            (100, 300, False),  # Within limit
            (300, 300, False),  # At limit
            (301, 300, True),  # Exceeded
            (600, 300, True),  # Well over
        ],
    )
    def test_timeout_detection_parametrized(self, elapsed, timeout, expected):
        """Test timeout detection with multiple scenarios."""
        from src.utils.timeout_handler import is_timeout_exceeded

        start_time = datetime(2024, 1, 1, 12, 0, 0)
        current_time = start_time + timedelta(seconds=elapsed)

        result = is_timeout_exceeded(start_time, current_time, timeout_seconds=timeout)
        assert result == expected
