"""
Completeness Checker Operator for Apache Airflow.

Validates row count against expected, minimum, and maximum thresholds
with tolerance percentage support.
"""

from typing import Any, Dict, Optional
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from src.operators.quality.base_quality_operator import BaseQualityOperator
from src.hooks.warehouse_hook import WarehouseHook
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CompletenessChecker(BaseQualityOperator):
    """
    Operator for checking data completeness via row counts.

    Validates that table has expected number of rows within
    acceptable min/max range or tolerance percentage.

    :param table_name: Table to check
    :param expected_count: Expected row count (optional)
    :param min_count: Minimum acceptable row count (optional)
    :param max_count: Maximum acceptable row count (optional)
    :param tolerance_percent: Tolerance as percentage of expected (optional)
    :param where_clause: SQL WHERE clause for filtering (optional)
    :param compare_with_previous: Compare with previous execution (optional)
    :param comparison_tolerance_percent: Tolerance for period comparison (optional)
    """

    template_fields = BaseQualityOperator.template_fields + ("where_clause",)

    @apply_defaults
    def __init__(
        self,
        *,
        expected_count: Optional[int] = None,
        min_count: Optional[int] = None,
        max_count: Optional[int] = None,
        tolerance_percent: Optional[float] = None,
        where_clause: Optional[str] = None,
        partition_column: Optional[str] = None,
        partition_value: Optional[str] = None,
        compare_with_previous: bool = False,
        comparison_tolerance_percent: Optional[float] = None,
        **kwargs,
    ):
        """Initialize CompletenessChecker."""
        super().__init__(**kwargs)

        self.expected_count = expected_count
        self.min_count = min_count
        self.max_count = max_count
        self.tolerance_percent = tolerance_percent
        self.where_clause = where_clause
        self.partition_column = partition_column
        self.partition_value = partition_value
        self.compare_with_previous = compare_with_previous
        self.comparison_tolerance_percent = comparison_tolerance_percent

        # Calculate min/max from expected and tolerance if provided
        if expected_count and tolerance_percent and not min_count and not max_count:
            tolerance_value = int(expected_count * (tolerance_percent / 100.0))
            self.min_count = expected_count - tolerance_value
            self.max_count = expected_count + tolerance_value

    def get_row_count(self) -> int:
        """
        Get row count from table.

        :return: Number of rows matching criteria
        """
        hook = WarehouseHook(postgres_conn_id=self.warehouse_conn_id)

        # Build query
        query = f"SELECT COUNT(*) FROM {self.table_name}"

        # Add WHERE clause if provided
        conditions = []
        if self.where_clause:
            conditions.append(f"({self.where_clause})")

        if self.partition_column and self.partition_value:
            conditions.append(f"{self.partition_column} = '{self.partition_value}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        result = hook.get_first(query)
        return result[0] if result else 0

    def get_previous_count(self, context: Dict[str, Any]) -> Optional[int]:
        """
        Get row count from previous execution.

        :param context: Airflow context
        :return: Previous row count or None
        """
        # TODO: Implement by querying quality_check_results table
        # For now, return None
        return None

    def perform_check(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform completeness check.

        :param context: Airflow context
        :return: Check result dictionary
        """
        try:
            # Get actual row count
            actual_count = self.get_row_count()

            # Initialize result
            result = {
                "actual_count": actual_count,
                "expected_count": self.expected_count,
                "min_count": self.min_count,
                "max_count": self.max_count,
            }

            # Check against minimum
            min_check_passed = True
            if self.min_count is not None:
                min_check_passed = actual_count >= self.min_count

            # Check against maximum
            max_check_passed = True
            if self.max_count is not None:
                max_check_passed = actual_count <= self.max_count

            # Check against expected with tolerance
            expected_check_passed = True
            if self.expected_count is not None:
                if self.tolerance_percent:
                    tolerance_value = int(self.expected_count * (self.tolerance_percent / 100.0))
                    expected_check_passed = (
                        self.expected_count - tolerance_value <= actual_count <= self.expected_count + tolerance_value
                    )
                else:
                    expected_check_passed = actual_count == self.expected_count

            # Overall pass/fail
            passed = min_check_passed and max_check_passed and expected_check_passed

            # Compare with previous if requested
            if self.compare_with_previous:
                previous_count = self.get_previous_count(context)
                if previous_count is not None:
                    result["previous_count"] = previous_count
                    percent_change = ((actual_count - previous_count) / previous_count * 100.0) if previous_count > 0 else 0.0
                    result["percent_change"] = round(percent_change, 2)

                    if self.comparison_tolerance_percent:
                        comparison_passed = abs(percent_change) <= self.comparison_tolerance_percent
                        passed = passed and comparison_passed

            # Calculate value score (0.0 - 1.0)
            if self.expected_count and self.expected_count > 0:
                value = min(1.0, actual_count / self.expected_count)
            else:
                value = 1.0 if passed else 0.0

            result.update({
                "passed": passed,
                "value": value,
                "expected": 1.0,
            })

            # Add message if failed
            if not passed:
                messages = []
                if not min_check_passed:
                    messages.append(f"Row count {actual_count} below minimum {self.min_count}")
                if not max_check_passed:
                    messages.append(f"Row count {actual_count} above maximum {self.max_count}")
                if not expected_check_passed:
                    messages.append(f"Row count {actual_count} differs from expected {self.expected_count}")

                result["message"] = "; ".join(messages)
                result["details"] = f"Completeness check failed for {self.table_name}"

            return result

        except Exception as e:
            logger.error(f"Completeness check error: {str(e)}", exc_info=True)
            raise AirflowException(f"Completeness check failed: {str(e)}") from e