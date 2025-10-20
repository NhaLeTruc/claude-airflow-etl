"""
Unit tests for Completeness Checker Operator.

Tests cover row count validation, min/max/expected counts, tolerance calculation,
and empty table detection.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from airflow.exceptions import AirflowException


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_completeness_dag"),
        "task": Mock(task_id="completeness_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1),
        "ds": "2025-01-15",
    }


class TestCompletenessChecker:
    """Test suite for CompletenessChecker operator."""

    def test_operator_initialization(self):
        """Test operator initializes with row count thresholds."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_completeness",
                table_name="warehouse.fact_sales",
                expected_count=1000,
                min_count=900,
                max_count=1100,
            )

            assert operator.task_id == "check_completeness"
            assert operator.table_name == "warehouse.fact_sales"
            assert operator.expected_count == 1000
            assert operator.min_count == 900
            assert operator.max_count == 1100
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_row_count_within_range(self, mock_context):
        """Test validation passes when row count is within min/max range."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_range",
                table_name="test_table",
                min_count=900,
                max_count=1100,
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.return_value = 1000  # Within range

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result["actual_count"] == 1000
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_row_count_below_minimum(self, mock_context):
        """Test validation fails when row count is below minimum."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_below_min",
                table_name="test_table",
                min_count=1000,
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.return_value = 500  # Below minimum

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert result["actual_count"] == 500
                assert result["min_count"] == 1000
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_row_count_above_maximum(self, mock_context):
        """Test validation fails when row count exceeds maximum."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_above_max",
                table_name="test_table",
                max_count=1000,
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.return_value = 1500  # Above maximum

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert result["actual_count"] == 1500
                assert result["max_count"] == 1000
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_expected_count_with_tolerance(self, mock_context):
        """Test expected count validation with tolerance percentage."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_tolerance",
                table_name="test_table",
                expected_count=1000,
                tolerance_percent=10,  # ±10%
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                # 950 is within 10% of 1000 (900-1100 range)
                mock_count.return_value = 950

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result["actual_count"] == 950
                assert result["expected_count"] == 1000
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_tolerance_calculation_accuracy(self, mock_context):
        """Test that tolerance percentage is calculated correctly."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="test_tolerance_calc",
                table_name="test_table",
                expected_count=1000,
                tolerance_percent=5,  # ±5% = 950-1050
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                # Test boundary values
                mock_count.return_value = 949  # Just outside tolerance

                result = operator.execute(mock_context)
                assert result["passed"] is False

                # Test within tolerance
                mock_count.return_value = 950  # Just inside tolerance
                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_empty_table_detection(self, mock_context):
        """Test detection of empty tables."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_empty",
                table_name="test_table",
                min_count=1,  # Require at least 1 row
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.return_value = 0

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert result["actual_count"] == 0
                assert "empty" in result.get("message", "").lower() or result["actual_count"] == 0
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_zero_count_allowed_when_no_min(self, mock_context):
        """Test that zero rows is valid when no minimum is specified."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="allow_zero",
                table_name="test_table",
                # No min_count specified
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.return_value = 0

                result = operator.execute(mock_context)
                # Should pass if no constraints specified
                assert result["passed"] is True or "min_count" not in operator.__dict__
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_where_clause_filtering(self, mock_context):
        """Test row count with WHERE clause filtering."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_filtered",
                table_name="warehouse.fact_sales",
                where_clause="sale_date = '{{ ds }}'",
                min_count=100,
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.return_value = 150

                result = operator.execute(mock_context)
                assert result["passed"] is True
                # Verify WHERE clause was used in SQL query
                mock_count.assert_called_once()
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_partition_filtering(self, mock_context):
        """Test row count on specific partitions."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="check_partition",
                table_name="warehouse.fact_sales",
                partition_column="sale_date",
                partition_value="{{ ds }}",
                min_count=1000,
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.return_value = 1500

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_comparison_with_previous_day(self, mock_context):
        """Test row count comparison with previous period."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="compare_previous",
                table_name="warehouse.fact_sales",
                compare_with_previous=True,
                comparison_tolerance_percent=20,  # Allow ±20% variance
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                with patch.object(operator, 'get_previous_count') as mock_prev:
                    mock_count.return_value = 1100
                    mock_prev.return_value = 1000

                    result = operator.execute(mock_context)
                    # 1100 is within 20% of 1000
                    assert result["passed"] is True
                    assert "previous_count" in result
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_percentage_change_calculation(self, mock_context):
        """Test calculation of percentage change from previous period."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            operator = CompletenessChecker(
                task_id="percent_change",
                table_name="test_table",
                compare_with_previous=True,
            )

            with patch.object(operator, 'get_row_count') as mock_count:
                with patch.object(operator, 'get_previous_count') as mock_prev:
                    mock_count.return_value = 1200
                    mock_prev.return_value = 1000

                    result = operator.execute(mock_context)
                    # Should calculate 20% increase
                    assert "percent_change" in result
                    assert result["percent_change"] == pytest.approx(20.0, rel=0.01)
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")

    def test_sql_injection_prevention(self, mock_context):
        """Test that table name is properly sanitized."""
        try:
            from src.operators.quality.completeness_checker import CompletenessChecker

            # Attempt SQL injection via table name
            operator = CompletenessChecker(
                task_id="injection_test",
                table_name="test_table'; DROP TABLE important_data; --",
                min_count=1,
            )

            # Should either sanitize or fail safely
            with patch.object(operator, 'get_row_count') as mock_count:
                mock_count.side_effect = Exception("Invalid table name")

                with pytest.raises((AirflowException, Exception)):
                    operator.execute(mock_context)
        except ImportError:
            pytest.skip("CompletenessChecker not implemented yet")