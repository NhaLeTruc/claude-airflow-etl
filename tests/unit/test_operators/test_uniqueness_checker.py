"""
Unit tests for Uniqueness Checker Operator.

Tests cover duplicate detection, composite key support, uniqueness validation,
and duplicate row identification.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_uniqueness_dag"),
        "task": Mock(task_id="uniqueness_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1),
        "ds": "2025-01-15",
    }


class TestUniquenessChecker:
    """Test suite for UniquenessChecker operator."""

    def test_operator_initialization(self):
        """Test operator initializes with key columns."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_uniqueness",
                table_name="warehouse.dim_customer",
                key_columns=["customer_key"],
            )

            assert operator.task_id == "check_uniqueness"
            assert operator.table_name == "warehouse.dim_customer"
            assert operator.key_columns == ["customer_key"]
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_no_duplicates_passes(self, mock_context):
        """Test validation passes when no duplicates found."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_unique",
                table_name="test_table",
                key_columns=["id"],
            )

            with patch.object(operator, "get_duplicate_count") as mock_count:
                mock_count.return_value = 0

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result["duplicate_count"] == 0
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_duplicates_detected(self, mock_context):
        """Test validation fails when duplicates are found."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_duplicates",
                table_name="test_table",
                key_columns=["customer_id"],
            )

            with patch.object(operator, "get_duplicate_count") as mock_count:
                mock_count.return_value = 5

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert result["duplicate_count"] == 5
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_composite_key_uniqueness(self, mock_context):
        """Test uniqueness check on composite keys."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_composite",
                table_name="warehouse.fact_sales",
                key_columns=["customer_id", "product_id", "sale_date_id"],
            )

            with patch.object(operator, "get_duplicate_count") as mock_count:
                mock_count.return_value = 0

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_duplicate_row_identification(self, mock_context):
        """Test identification of specific duplicate rows."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="identify_duplicates",
                table_name="test_table",
                key_columns=["email"],
                return_duplicates=True,
            )

            duplicate_rows = [
                {"email": "test@example.com", "count": 3},
                {"email": "duplicate@example.com", "count": 2},
            ]

            with patch.object(operator, "get_duplicate_rows") as mock_rows:
                mock_rows.return_value = duplicate_rows

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert len(result["duplicate_rows"]) == 2
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_duplicate_percentage_calculation(self, mock_context):
        """Test calculation of duplicate percentage."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="calc_percentage",
                table_name="test_table",
                key_columns=["id"],
            )

            with patch.object(operator, "get_duplicate_count") as mock_dup:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_dup.return_value = 10
                    mock_total.return_value = 1000

                    result = operator.execute(mock_context)
                    # 10 duplicates out of 1000 = 1%
                    assert result["duplicate_percentage"] == pytest.approx(1.0, rel=0.01)
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_threshold_tolerance(self, mock_context):
        """Test validation with duplicate threshold tolerance."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_threshold",
                table_name="test_table",
                key_columns=["id"],
                max_duplicate_percentage=5.0,  # Allow up to 5% duplicates
            )

            with patch.object(operator, "get_duplicate_count") as mock_dup:
                with patch.object(operator, "get_total_count") as mock_total:
                    mock_dup.return_value = 3
                    mock_total.return_value = 100  # 3% duplicates

                    result = operator.execute(mock_context)
                    # Should pass (3% < 5% threshold)
                    assert result["passed"] is True
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_where_clause_filtering(self, mock_context):
        """Test uniqueness check with WHERE clause."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_filtered",
                table_name="warehouse.fact_sales",
                key_columns=["transaction_id"],
                where_clause="sale_date = '{{ ds }}'",
            )

            with patch.object(operator, "get_duplicate_count") as mock_count:
                mock_count.return_value = 0

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_null_handling_in_keys(self, mock_context):
        """Test handling of NULL values in key columns."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_nulls",
                table_name="test_table",
                key_columns=["email"],
                exclude_nulls=True,
            )

            with patch.object(operator, "get_duplicate_count") as mock_count:
                mock_count.return_value = 0

                result = operator.execute(mock_context)
                # Should pass when NULLs are excluded
                assert result["passed"] is True
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_case_sensitive_uniqueness(self, mock_context):
        """Test case-sensitive uniqueness checking."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_case_sensitive",
                table_name="test_table",
                key_columns=["username"],
                case_sensitive=True,
            )

            with patch.object(operator, "get_duplicate_count") as mock_count:
                # "User" and "user" should be treated as different
                mock_count.return_value = 0

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")

    def test_trim_whitespace_option(self, mock_context):
        """Test trimming whitespace before uniqueness check."""
        try:
            from src.operators.quality.uniqueness_checker import UniquenessChecker

            operator = UniquenessChecker(
                task_id="check_trim",
                table_name="test_table",
                key_columns=["code"],
                trim_whitespace=True,
            )

            with patch.object(operator, "get_duplicate_count") as mock_count:
                # "ABC" and " ABC " should be treated as duplicates
                mock_count.return_value = 1

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert result["duplicate_count"] == 1
        except ImportError:
            pytest.skip("UniquenessChecker not implemented yet")
