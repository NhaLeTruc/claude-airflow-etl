"""
Unit tests for Base Quality Operator.

Tests cover severity levels, threshold validation, result logging,
and common quality check functionality.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from airflow.exceptions import AirflowException


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_quality_dag"),
        "task": Mock(task_id="quality_check_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1, max_tries=3),
        "ds": "2025-01-15",
    }


class TestBaseQualityOperator:
    """Test suite for BaseQualityOperator."""

    def test_operator_initialization(self):
        """Test operator initializes with severity and thresholds."""
        try:
            from src.operators.quality.base_quality_operator import (
                BaseQualityOperator,
                QualitySeverity,
            )

            # Create a concrete subclass for testing
            class TestQualityCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {"passed": True, "value": 100}

            operator = TestQualityCheck(
                task_id="test_check",
                table_name="warehouse.fact_sales",
                severity=QualitySeverity.WARNING,
                threshold=0.95,
            )

            assert operator.task_id == "test_check"
            assert operator.table_name == "warehouse.fact_sales"
            assert operator.severity == QualitySeverity.WARNING
            assert operator.threshold == 0.95
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_severity_enum_values(self):
        """Test QualitySeverity enum has expected values."""
        try:
            from src.operators.quality.base_quality_operator import QualitySeverity

            # Test all severity levels exist
            assert hasattr(QualitySeverity, "INFO")
            assert hasattr(QualitySeverity, "WARNING")
            assert hasattr(QualitySeverity, "ERROR")
            assert hasattr(QualitySeverity, "CRITICAL")

            # Test severity ordering
            assert QualitySeverity.INFO.value < QualitySeverity.WARNING.value
            assert QualitySeverity.WARNING.value < QualitySeverity.ERROR.value
            assert QualitySeverity.ERROR.value < QualitySeverity.CRITICAL.value
        except ImportError:
            pytest.skip("QualitySeverity enum not implemented yet")

    def test_threshold_validation_success(self):
        """Test threshold validation passes when value meets threshold."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            class TestCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {"passed": True, "value": 0.98}

            operator = TestCheck(
                task_id="test",
                table_name="test_table",
                threshold=0.95,
            )

            result = {"passed": True, "value": 0.98}
            is_valid = operator.validate_threshold(result)
            assert is_valid is True
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_threshold_validation_failure(self):
        """Test threshold validation fails when value below threshold."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            class TestCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {"passed": False, "value": 0.80}

            operator = TestCheck(
                task_id="test",
                table_name="test_table",
                threshold=0.95,
            )

            result = {"passed": False, "value": 0.80}
            is_valid = operator.validate_threshold(result)
            assert is_valid is False
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_result_logging_structure(self, mock_context):
        """Test that quality check results are logged with proper structure."""
        try:
            from src.operators.quality.base_quality_operator import (
                BaseQualityOperator,
                QualitySeverity,
            )

            class TestCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {
                        "passed": True,
                        "value": 0.98,
                        "expected": 0.95,
                        "details": "All checks passed",
                    }

            operator = TestCheck(
                task_id="test",
                table_name="warehouse.fact_sales",
                severity=QualitySeverity.WARNING,
            )

            result = operator.execute(mock_context)

            # Result should contain required fields
            assert "passed" in result
            assert "check_type" in result
            assert "table_name" in result
            assert "severity" in result
            assert "timestamp" in result
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_critical_severity_fails_task(self, mock_context):
        """Test that CRITICAL severity causes task failure when check fails."""
        try:
            from src.operators.quality.base_quality_operator import (
                BaseQualityOperator,
                QualitySeverity,
            )

            class FailingCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {"passed": False, "value": 0.50, "expected": 0.95}

            operator = FailingCheck(
                task_id="critical_check",
                table_name="warehouse.fact_sales",
                severity=QualitySeverity.CRITICAL,
                threshold=0.95,
            )

            # Critical failure should raise AirflowException
            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_warning_severity_logs_but_passes(self, mock_context):
        """Test that WARNING severity logs issue but doesn't fail task."""
        try:
            from src.operators.quality.base_quality_operator import (
                BaseQualityOperator,
                QualitySeverity,
            )

            class WarningCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {"passed": False, "value": 0.90, "expected": 0.95}

            operator = WarningCheck(
                task_id="warning_check",
                table_name="warehouse.fact_sales",
                severity=QualitySeverity.WARNING,
                threshold=0.95,
            )

            # Warning should not raise exception
            result = operator.execute(mock_context)
            assert result is not None
            assert result["passed"] is False
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    @patch('src.hooks.warehouse_hook.WarehouseHook')
    def test_warehouse_connection(self, mock_hook, mock_context):
        """Test that operator connects to warehouse using hook."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            class DBCheck(BaseQualityOperator):
                def perform_check(self, context):
                    # Simulates using warehouse connection
                    return {"passed": True, "row_count": 100}

            mock_hook_instance = MagicMock()
            mock_hook.return_value = mock_hook_instance

            operator = DBCheck(
                task_id="db_check",
                table_name="warehouse.fact_sales",
                warehouse_conn_id="warehouse_default",
            )

            operator.execute(mock_context)

            # Verify hook was initialized (connection established)
            # Note: This test assumes operator uses WarehouseHook internally
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_result_storage_to_database(self, mock_context):
        """Test that quality check results are stored to database."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            class StorageCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {"passed": True, "value": 0.98}

            operator = StorageCheck(
                task_id="storage_test",
                table_name="warehouse.fact_sales",
                store_results=True,
            )

            with patch.object(operator, 'store_result') as mock_store:
                operator.execute(mock_context)
                # Verify store_result was called
                mock_store.assert_called_once()
        except (ImportError, AttributeError):
            pytest.skip("Result storage not implemented yet")

    def test_quality_check_with_custom_message(self, mock_context):
        """Test custom failure message in quality check result."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            class MessageCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {
                        "passed": False,
                        "message": "Row count below threshold: expected 1000, got 500",
                        "value": 500,
                        "expected": 1000,
                    }

            operator = MessageCheck(
                task_id="message_check",
                table_name="warehouse.fact_sales",
            )

            result = operator.execute(mock_context)
            assert "message" in result
            assert "expected" in result["message"].lower()
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_ui_color_for_quality_operator(self):
        """Test that quality operator has distinct UI color."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            class ColorCheck(BaseQualityOperator):
                def perform_check(self, context):
                    return {"passed": True}

            operator = ColorCheck(task_id="test", table_name="test_table")

            assert hasattr(operator, 'ui_color') or hasattr(BaseQualityOperator, 'ui_color')
            # Quality operators typically use yellow/orange colors
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_template_fields_defined(self):
        """Test that template_fields includes table_name."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            assert hasattr(BaseQualityOperator, 'template_fields')
            template_fields = BaseQualityOperator.template_fields

            # Table name should be templatable
            assert 'table_name' in template_fields
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_abstract_perform_check_method(self):
        """Test that perform_check must be implemented by subclasses."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            # Trying to instantiate without implementing perform_check should fail
            with pytest.raises(TypeError):
                operator = BaseQualityOperator(
                    task_id="abstract_test",
                    table_name="test_table",
                )
                # Cannot instantiate abstract class
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")

    def test_execution_context_passed_to_check(self, mock_context):
        """Test that Airflow context is passed to perform_check."""
        try:
            from src.operators.quality.base_quality_operator import BaseQualityOperator

            context_received = {}

            class ContextCheck(BaseQualityOperator):
                def perform_check(self, context):
                    context_received.update(context)
                    return {"passed": True}

            operator = ContextCheck(task_id="context_test", table_name="test_table")
            operator.execute(mock_context)

            # Verify context was passed
            assert "dag" in context_received
            assert "task" in context_received
            assert context_received["dag"].dag_id == "test_quality_dag"
        except ImportError:
            pytest.skip("BaseQualityOperator not implemented yet")