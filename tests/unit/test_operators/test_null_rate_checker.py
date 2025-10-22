"""
Unit tests for Null Rate Checker Operator.

Tests cover NULL percentage calculation, threshold validation,
multi-column checking, and NOT NULL constraint verification.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_null_rate_dag"),
        "task": Mock(task_id="null_rate_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1),
        "ds": "2025-01-15",
    }


class TestNullRateChecker:
    """Test suite for NullRateChecker operator."""

    def test_operator_initialization(self):
        """Test operator initializes with column and threshold."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="check_null_rate",
                table_name="warehouse.dim_customer",
                column_name="email",
                max_null_percentage=10.0,
            )

            assert operator.task_id == "check_null_rate"
            assert operator.table_name == "warehouse.dim_customer"
            assert operator.column_name == "email"
            assert operator.max_null_percentage == 10.0
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_low_null_rate_passes(self, mock_context):
        """Test validation passes when null rate is below threshold."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="check_low_nulls",
                table_name="test_table",
                column_name="email",
                max_null_percentage=10.0,
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 5
                    mock_total.return_value = 1000  # 0.5% null rate

                    result = operator.execute(mock_context)
                    assert result["passed"] is True
                    assert result["null_percentage"] < 10.0
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_high_null_rate_fails(self, mock_context):
        """Test validation fails when null rate exceeds threshold."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="check_high_nulls",
                table_name="test_table",
                column_name="phone",
                max_null_percentage=10.0,
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 200
                    mock_total.return_value = 1000  # 20% null rate

                    result = operator.execute(mock_context)
                    assert result["passed"] is False
                    assert result["null_percentage"] == pytest.approx(20.0, rel=0.01)
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_null_percentage_calculation(self, mock_context):
        """Test accurate null percentage calculation."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="calc_null_pct",
                table_name="test_table",
                column_name="field",
                max_null_percentage=25.0,
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 250
                    mock_total.return_value = 1000  # Exactly 25%

                    result = operator.execute(mock_context)
                    assert result["null_percentage"] == pytest.approx(25.0, rel=0.01)
                    # Should pass at boundary
                    assert result["passed"] is True
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_zero_nulls(self, mock_context):
        """Test handling of columns with no null values."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="zero_nulls",
                table_name="test_table",
                column_name="id",
                max_null_percentage=0.0,  # Require no NULLs
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 0
                    mock_total.return_value = 1000

                    result = operator.execute(mock_context)
                    assert result["passed"] is True
                    assert result["null_percentage"] == 0.0
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_all_nulls(self, mock_context):
        """Test handling of columns with all null values."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="all_nulls",
                table_name="test_table",
                column_name="optional_field",
                max_null_percentage=50.0,
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 100
                    mock_total.return_value = 100  # 100% null

                    result = operator.execute(mock_context)
                    assert result["passed"] is False
                    assert result["null_percentage"] == 100.0
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_multiple_columns_check(self, mock_context):
        """Test null rate checking across multiple columns."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="multi_column_nulls",
                table_name="test_table",
                columns=["email", "phone", "address"],
                max_null_percentage=10.0,
            )

            with patch.object(operator, "get_null_rates") as mock_rates:
                mock_rates.return_value = {
                    "email": 5.0,
                    "phone": 8.0,
                    "address": 12.0,  # Exceeds threshold
                }

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert "address" in result["failed_columns"]
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_where_clause_filtering(self, mock_context):
        """Test null rate check with WHERE clause."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="filtered_nulls",
                table_name="warehouse.fact_sales",
                column_name="discount",
                where_clause="sale_date = '{{ ds }}'",
                max_null_percentage=5.0,
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 2
                    mock_total.return_value = 100

                    result = operator.execute(mock_context)
                    assert result["passed"] is True
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_empty_table_handling(self, mock_context):
        """Test null rate check on empty table."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="empty_table",
                table_name="test_table",
                column_name="field",
                max_null_percentage=10.0,
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 0
                    mock_total.return_value = 0  # Empty table

                    result = operator.execute(mock_context)
                    # Should handle division by zero
                    assert "null_percentage" in result
                    # Could be N/A or 0% depending on implementation
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_not_null_constraint_validation(self, mock_context):
        """Test validation that NOT NULL columns have no nulls."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="validate_not_null",
                table_name="test_table",
                column_name="id",
                expect_not_null=True,  # Column should have NOT NULL constraint
            )

            with patch.object(operator, "get_null_count") as mock_null:
                mock_null.return_value = 0

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")

    def test_detailed_null_statistics(self, mock_context):
        """Test that result includes detailed null statistics."""
        try:
            from src.operators.quality.null_rate_checker import NullRateChecker

            operator = NullRateChecker(
                task_id="null_stats",
                table_name="test_table",
                column_name="email",
                max_null_percentage=10.0,
            )

            with patch.object(operator, "get_null_count") as mock_null:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_null.return_value = 50
                    mock_total.return_value = 1000

                    result = operator.execute(mock_context)
                    assert "null_count" in result
                    assert "total_count" in result
                    assert "null_percentage" in result
                    assert "non_null_count" in result
                    assert result["non_null_count"] == 950
        except ImportError:
            pytest.skip("NullRateChecker not implemented yet")
