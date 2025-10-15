"""
Unit tests for retry policy utility functions.

Tests exponential backoff calculation, max retries enforcement,
and retry delay generation.
"""

from datetime import timedelta

import pytest


class TestRetryPolicies:
    """Test suite for retry policy utility functions."""

    def test_exponential_backoff_calculation(self):
        """Test exponential backoff delay calculation."""
        from src.utils.retry_policies import calculate_exponential_backoff

        # Test base case (retry 0)
        delay = calculate_exponential_backoff(retry_number=0, base_delay=60)
        assert delay == 60  # 60 * 2^0 = 60

        # Test retry 1
        delay = calculate_exponential_backoff(retry_number=1, base_delay=60)
        assert delay == 120  # 60 * 2^1 = 120

        # Test retry 2
        delay = calculate_exponential_backoff(retry_number=2, base_delay=60)
        assert delay == 240  # 60 * 2^2 = 240

        # Test retry 3
        delay = calculate_exponential_backoff(retry_number=3, base_delay=60)
        assert delay == 480  # 60 * 2^3 = 480

    def test_exponential_backoff_with_custom_base(self):
        """Test exponential backoff with different base delays."""
        from src.utils.retry_policies import calculate_exponential_backoff

        # Base delay 30 seconds
        delay = calculate_exponential_backoff(retry_number=2, base_delay=30)
        assert delay == 120  # 30 * 2^2 = 120

        # Base delay 300 seconds (5 minutes)
        delay = calculate_exponential_backoff(retry_number=1, base_delay=300)
        assert delay == 600  # 300 * 2^1 = 600

    def test_exponential_backoff_with_max_delay(self):
        """Test that exponential backoff respects maximum delay."""
        from src.utils.retry_policies import calculate_exponential_backoff

        # Without max, would be 3840 (60 * 2^6)
        delay = calculate_exponential_backoff(retry_number=6, base_delay=60, max_delay=1800)
        assert delay == 1800  # Capped at max_delay

        # Below max, should use calculated value
        delay = calculate_exponential_backoff(retry_number=2, base_delay=60, max_delay=1800)
        assert delay == 240  # 60 * 2^2 = 240 (below max)

    def test_exponential_backoff_returns_timedelta(self):
        """Test that exponential backoff can return timedelta."""
        from src.utils.retry_policies import calculate_exponential_backoff

        delay = calculate_exponential_backoff(retry_number=1, base_delay=60, return_timedelta=True)
        assert isinstance(delay, timedelta)
        assert delay.total_seconds() == 120

    def test_max_retries_enforcement(self):
        """Test that max retries are enforced."""
        from src.utils.retry_policies import should_retry

        # Should retry within limit
        assert should_retry(current_attempt=1, max_retries=3) is True
        assert should_retry(current_attempt=2, max_retries=3) is True
        assert should_retry(current_attempt=3, max_retries=3) is True

        # Should not retry after limit
        assert should_retry(current_attempt=4, max_retries=3) is False
        assert should_retry(current_attempt=5, max_retries=3) is False

    def test_max_retries_zero_means_no_retries(self):
        """Test that max_retries=0 means no retries."""
        from src.utils.retry_policies import should_retry

        assert should_retry(current_attempt=1, max_retries=0) is False

    def test_max_retries_one_allows_one_retry(self):
        """Test that max_retries=1 allows exactly one retry."""
        from src.utils.retry_policies import should_retry

        assert should_retry(current_attempt=1, max_retries=1) is True
        assert should_retry(current_attempt=2, max_retries=1) is False

    def test_generate_retry_delays_sequence(self):
        """Test generation of retry delay sequence."""
        from src.utils.retry_policies import generate_retry_delays

        delays = generate_retry_delays(max_retries=3, base_delay=60)

        assert len(delays) == 3
        assert delays[0] == 60  # Retry 1: 60 * 2^0
        assert delays[1] == 120  # Retry 2: 60 * 2^1
        assert delays[2] == 240  # Retry 3: 60 * 2^2

    def test_generate_retry_delays_with_max(self):
        """Test retry delay sequence with maximum cap."""
        from src.utils.retry_policies import generate_retry_delays

        delays = generate_retry_delays(max_retries=5, base_delay=60, max_delay=200)

        assert len(delays) == 5
        assert delays[0] == 60  # 60 * 2^0 = 60
        assert delays[1] == 120  # 60 * 2^1 = 120
        assert delays[2] == 200  # Capped at 200 (would be 240)
        assert delays[3] == 200  # Capped at 200 (would be 480)
        assert delays[4] == 200  # Capped at 200 (would be 960)

    def test_generate_retry_delays_as_timedeltas(self):
        """Test retry delays as timedelta objects."""
        from src.utils.retry_policies import generate_retry_delays

        delays = generate_retry_delays(max_retries=2, base_delay=60, return_timedeltas=True)

        assert len(delays) == 2
        assert isinstance(delays[0], timedelta)
        assert isinstance(delays[1], timedelta)
        assert delays[0].total_seconds() == 60
        assert delays[1].total_seconds() == 120

    def test_linear_backoff_calculation(self):
        """Test linear backoff delay calculation."""
        from src.utils.retry_policies import calculate_linear_backoff

        # Linear: retry_number * base_delay
        assert calculate_linear_backoff(retry_number=0, base_delay=60) == 60
        assert calculate_linear_backoff(retry_number=1, base_delay=60) == 120
        assert calculate_linear_backoff(retry_number=2, base_delay=60) == 180
        assert calculate_linear_backoff(retry_number=3, base_delay=60) == 240

    def test_linear_backoff_with_max_delay(self):
        """Test linear backoff respects maximum delay."""
        from src.utils.retry_policies import calculate_linear_backoff

        delay = calculate_linear_backoff(retry_number=10, base_delay=60, max_delay=300)
        assert delay == 300  # Capped at max (would be 660)

    def test_fixed_backoff_calculation(self):
        """Test fixed backoff (constant delay)."""
        from src.utils.retry_policies import calculate_fixed_backoff

        # Fixed delay always returns base_delay
        assert calculate_fixed_backoff(retry_number=0, delay=120) == 120
        assert calculate_fixed_backoff(retry_number=1, delay=120) == 120
        assert calculate_fixed_backoff(retry_number=5, delay=120) == 120

    def test_get_retry_delay_for_airflow_task(self):
        """Test getting retry delay for Airflow task configuration."""
        from src.utils.retry_policies import get_retry_delay_for_airflow

        # Should return timedelta for Airflow
        delay = get_retry_delay_for_airflow(retry_number=1, strategy="exponential", base_delay=60)

        assert isinstance(delay, timedelta)
        assert delay.total_seconds() == 120

    def test_retry_strategy_selection(self):
        """Test different retry strategies."""
        from src.utils.retry_policies import get_retry_delay_for_airflow

        # Exponential
        exp_delay = get_retry_delay_for_airflow(
            retry_number=2, strategy="exponential", base_delay=60
        )
        assert exp_delay.total_seconds() == 240

        # Linear
        lin_delay = get_retry_delay_for_airflow(retry_number=2, strategy="linear", base_delay=60)
        assert lin_delay.total_seconds() == 180

        # Fixed
        fix_delay = get_retry_delay_for_airflow(retry_number=2, strategy="fixed", base_delay=60)
        assert fix_delay.total_seconds() == 60

    def test_invalid_retry_strategy_raises_error(self):
        """Test that invalid retry strategy raises error."""
        from src.utils.retry_policies import get_retry_delay_for_airflow

        with pytest.raises(ValueError) as exc_info:
            get_retry_delay_for_airflow(retry_number=1, strategy="invalid", base_delay=60)

        assert "strategy" in str(exc_info.value).lower()

    def test_negative_retry_number_raises_error(self):
        """Test that negative retry numbers raise error."""
        from src.utils.retry_policies import calculate_exponential_backoff

        with pytest.raises(ValueError):
            calculate_exponential_backoff(retry_number=-1, base_delay=60)

    def test_zero_base_delay_raises_error(self):
        """Test that zero or negative base delay raises error."""
        from src.utils.retry_policies import calculate_exponential_backoff

        with pytest.raises(ValueError):
            calculate_exponential_backoff(retry_number=1, base_delay=0)

        with pytest.raises(ValueError):
            calculate_exponential_backoff(retry_number=1, base_delay=-10)

    def test_retry_policy_configuration_dict(self):
        """Test creating retry policy configuration dict."""
        from src.utils.retry_policies import create_retry_config

        config = create_retry_config(max_retries=3, strategy="exponential", base_delay=60)

        assert config["retries"] == 3
        assert config["retry_delay"].total_seconds() == 60
        assert config["retry_exponential_backoff"] is True

    def test_retry_config_for_linear_strategy(self):
        """Test retry config for linear strategy."""
        from src.utils.retry_policies import create_retry_config

        config = create_retry_config(max_retries=5, strategy="linear", base_delay=120)

        assert config["retries"] == 5
        assert config["retry_delay"].total_seconds() == 120
        assert config["retry_exponential_backoff"] is False

    @pytest.mark.parametrize(
        "retry_num,base,expected",
        [
            (0, 30, 30),
            (1, 30, 60),
            (2, 30, 120),
            (3, 30, 240),
            (4, 30, 480),
        ],
    )
    def test_exponential_backoff_parametrized(self, retry_num, base, expected):
        """Test exponential backoff with multiple parameters."""
        from src.utils.retry_policies import calculate_exponential_backoff

        delay = calculate_exponential_backoff(retry_number=retry_num, base_delay=base)
        assert delay == expected
