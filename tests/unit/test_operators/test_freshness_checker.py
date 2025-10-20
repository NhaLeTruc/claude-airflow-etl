"""
Unit tests for Freshness Checker Operator.

Tests cover timestamp comparison, max age validation, SLA compliance,
and staleness detection.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_freshness_dag"),
        "task": Mock(task_id="freshness_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1),
        "ds": "2025-01-15",
    }


class TestFreshnessChecker:
    """Test suite for FreshnessChecker operator."""

    def test_operator_initialization(self):
        """Test operator initializes with freshness parameters."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_freshness",
                table_name="warehouse.fact_sales",
                timestamp_column="updated_at",
                max_age_hours=24,
            )

            assert operator.task_id == "check_freshness"
            assert operator.table_name == "warehouse.fact_sales"
            assert operator.timestamp_column == "updated_at"
            assert operator.max_age_hours == 24
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_fresh_data_passes(self, mock_context):
        """Test validation passes when data is fresh."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_fresh",
                table_name="test_table",
                timestamp_column="created_at",
                max_age_hours=24,
            )

            # Data from 1 hour ago (fresh)
            recent_timestamp = datetime.now() - timedelta(hours=1)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = recent_timestamp

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result["age_hours"] < 24
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_stale_data_fails(self, mock_context):
        """Test validation fails when data is stale."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_stale",
                table_name="test_table",
                timestamp_column="updated_at",
                max_age_hours=24,
            )

            # Data from 48 hours ago (stale)
            old_timestamp = datetime.now() - timedelta(hours=48)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = old_timestamp

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert result["age_hours"] > 24
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_age_calculation_in_hours(self, mock_context):
        """Test accurate age calculation in hours."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_age_calc",
                table_name="test_table",
                timestamp_column="timestamp",
                max_age_hours=24,
            )

            # Data from exactly 12 hours ago
            timestamp_12h_ago = datetime.now() - timedelta(hours=12)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = timestamp_12h_ago

                result = operator.execute(mock_context)
                assert result["age_hours"] == pytest.approx(12.0, abs=0.1)
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_age_in_minutes(self, mock_context):
        """Test age reporting in minutes for very fresh data."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_minutes",
                table_name="test_table",
                timestamp_column="timestamp",
                max_age_minutes=30,
            )

            # Data from 15 minutes ago
            timestamp_15m_ago = datetime.now() - timedelta(minutes=15)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = timestamp_15m_ago

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result["age_minutes"] == pytest.approx(15.0, abs=1.0)
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_sla_compliance(self, mock_context):
        """Test SLA compliance checking."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_sla",
                table_name="warehouse.fact_sales",
                timestamp_column="loaded_at",
                sla_hours=2,  # Must be loaded within 2 hours
            )

            # Data loaded 1 hour ago (within SLA)
            timestamp_1h_ago = datetime.now() - timedelta(hours=1)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = timestamp_1h_ago

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result["sla_compliant"] is True
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_null_timestamp_handling(self, mock_context):
        """Test handling of NULL timestamps."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_null_ts",
                table_name="test_table",
                timestamp_column="updated_at",
                max_age_hours=24,
            )

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = None  # NULL timestamp

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert "no_data" in result or result.get("max_timestamp") is None
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_empty_table_detection(self, mock_context):
        """Test detection of empty tables with no timestamps."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_empty",
                table_name="test_table",
                timestamp_column="created_at",
                max_age_hours=24,
            )

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = None

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert "empty" in result.get("message", "").lower() or result.get("row_count") == 0
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_where_clause_filtering(self, mock_context):
        """Test freshness check with WHERE clause filtering."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_filtered_fresh",
                table_name="warehouse.fact_sales",
                timestamp_column="sale_date",
                where_clause="status = 'completed'",
                max_age_hours=24,
            )

            recent_ts = datetime.now() - timedelta(hours=12)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = recent_ts

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_timezone_aware_timestamps(self, mock_context):
        """Test handling of timezone-aware timestamps."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker
            import pytz

            operator = FreshnessChecker(
                task_id="check_tz_aware",
                table_name="test_table",
                timestamp_column="created_at",
                max_age_hours=24,
            )

            # Timezone-aware timestamp (UTC)
            tz_timestamp = datetime.now(pytz.UTC) - timedelta(hours=12)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = tz_timestamp

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet or pytz not available")

    def test_future_timestamp_handling(self, mock_context):
        """Test handling of future timestamps (clock skew)."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_future_ts",
                table_name="test_table",
                timestamp_column="created_at",
                max_age_hours=24,
            )

            # Future timestamp (clock skew or data error)
            future_timestamp = datetime.now() + timedelta(hours=2)

            with patch.object(operator, 'get_max_timestamp') as mock_ts:
                mock_ts.return_value = future_timestamp

                result = operator.execute(mock_context)
                # Should either pass (age=0) or flag as anomaly
                assert "future" in result or result["age_hours"] <= 0 or result["passed"] is True
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")

    def test_multiple_timestamp_columns(self, mock_context):
        """Test checking freshness across multiple timestamp columns."""
        try:
            from src.operators.quality.freshness_checker import FreshnessChecker

            operator = FreshnessChecker(
                task_id="check_multi_ts",
                table_name="test_table",
                timestamp_columns=["created_at", "updated_at", "processed_at"],
                max_age_hours=24,
            )

            with patch.object(operator, 'get_max_timestamps') as mock_ts:
                mock_ts.return_value = {
                    "created_at": datetime.now() - timedelta(hours=48),
                    "updated_at": datetime.now() - timedelta(hours=12),  # Most recent
                    "processed_at": datetime.now() - timedelta(hours=36),
                }

                result = operator.execute(mock_context)
                # Should use most recent timestamp
                assert result["age_hours"] < 24
        except ImportError:
            pytest.skip("FreshnessChecker not implemented yet")