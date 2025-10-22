"""
Unit tests for Schema Validation Operator.

Tests cover column validation, data type checking, missing/extra column detection,
and schema compliance reporting.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from airflow.exceptions import AirflowException


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_schema_dag"),
        "task": Mock(task_id="schema_validation_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1),
        "ds": "2025-01-15",
    }


class TestSchemaValidator:
    """Test suite for SchemaValidator operator."""

    def test_operator_initialization(self):
        """Test operator initializes with schema definition."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "customer_id", "type": "INTEGER"},
                    {"name": "customer_name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"},
                ]
            }

            operator = SchemaValidator(
                task_id="validate_schema",
                table_name="warehouse.dim_customer",
                expected_schema=expected_schema,
            )

            assert operator.task_id == "validate_schema"
            assert operator.table_name == "warehouse.dim_customer"
            assert operator.expected_schema == expected_schema
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_column_presence_validation(self, mock_context):
        """Test validation of required columns presence."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ]
            }

            operator = SchemaValidator(
                task_id="check_columns",
                table_name="test_table",
                expected_schema=expected_schema,
            )

            # Mock database response with matching columns
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "name", "data_type": "character varying"},
                ]

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result["missing_columns"] == []
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_missing_columns_detection(self, mock_context):
        """Test detection of missing required columns."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"},
                ]
            }

            operator = SchemaValidator(
                task_id="check_missing",
                table_name="test_table",
                expected_schema=expected_schema,
            )

            # Mock database response missing 'email' column
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "name", "data_type": "character varying"},
                ]

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert "email" in result["missing_columns"]
                assert len(result["missing_columns"]) == 1
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_extra_columns_detection(self, mock_context):
        """Test detection of unexpected extra columns."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                ]
            }

            operator = SchemaValidator(
                task_id="check_extra",
                table_name="test_table",
                expected_schema=expected_schema,
                allow_extra_columns=False,
            )

            # Mock database response with extra column
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "name", "data_type": "character varying"},
                    {"column_name": "extra_col", "data_type": "text"},
                ]

                result = operator.execute(mock_context)
                assert "extra_columns" in result
                assert "extra_col" in result["extra_columns"]
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_data_type_validation(self, mock_context):
        """Test validation of column data types."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "amount", "type": "NUMERIC"},
                    {"name": "name", "type": "VARCHAR"},
                ]
            }

            operator = SchemaValidator(
                task_id="check_types",
                table_name="test_table",
                expected_schema=expected_schema,
                check_types=True,
            )

            # Mock database response with matching types
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "amount", "data_type": "numeric"},
                    {"column_name": "name", "data_type": "character varying"},
                ]

                result = operator.execute(mock_context)
                assert result["passed"] is True
                assert result.get("type_mismatches", []) == []
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_data_type_mismatch_detection(self, mock_context):
        """Test detection of data type mismatches."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "amount", "type": "NUMERIC"},
                ]
            }

            operator = SchemaValidator(
                task_id="check_type_mismatch",
                table_name="test_table",
                expected_schema=expected_schema,
                check_types=True,
            )

            # Mock database response with wrong type for 'amount'
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "amount", "data_type": "text"},  # Wrong type!
                ]

                result = operator.execute(mock_context)
                assert result["passed"] is False
                assert len(result["type_mismatches"]) > 0
                assert any("amount" in str(m) for m in result["type_mismatches"])
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_allow_extra_columns_flag(self, mock_context):
        """Test that allow_extra_columns flag controls strictness."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                ]
            }

            operator = SchemaValidator(
                task_id="allow_extra",
                table_name="test_table",
                expected_schema=expected_schema,
                allow_extra_columns=True,
            )

            # Mock database response with extra columns
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "integer"},
                    {"column_name": "extra1", "data_type": "text"},
                    {"column_name": "extra2", "data_type": "text"},
                ]

                result = operator.execute(mock_context)
                # Should pass when extra columns are allowed
                assert result["passed"] is True
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_column_order_validation(self, mock_context):
        """Test optional column order validation."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER", "position": 1},
                    {"name": "name", "type": "VARCHAR", "position": 2},
                ]
            }

            operator = SchemaValidator(
                task_id="check_order",
                table_name="test_table",
                expected_schema=expected_schema,
                check_column_order=True,
            )

            # Mock database response with different order
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {
                        "column_name": "name",
                        "data_type": "character varying",
                        "ordinal_position": 1,
                    },
                    {"column_name": "id", "data_type": "integer", "ordinal_position": 2},
                ]

                result = operator.execute(mock_context)
                # Order mismatch should be detected if check_column_order=True
                assert "order_mismatches" in result or result["passed"] is False
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_nullable_constraint_validation(self, mock_context):
        """Test validation of nullable/not-null constraints."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "email", "type": "VARCHAR", "nullable": True},
                ]
            }

            operator = SchemaValidator(
                task_id="check_nullable",
                table_name="test_table",
                expected_schema=expected_schema,
                check_nullability=True,
            )

            # Mock database response with matching nullability
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "integer", "is_nullable": "NO"},
                    {
                        "column_name": "email",
                        "data_type": "character varying",
                        "is_nullable": "YES",
                    },
                ]

                result = operator.execute(mock_context)
                assert result["passed"] is True
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_table_not_exists_error(self, mock_context):
        """Test error handling when table doesn't exist."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            operator = SchemaValidator(
                task_id="nonexistent_table",
                table_name="warehouse.nonexistent_table",
                expected_schema={"columns": []},
            )

            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.side_effect = Exception("Table does not exist")

                with pytest.raises(AirflowException):
                    operator.execute(mock_context)
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")

    def test_detailed_error_reporting(self, mock_context):
        """Test that validation provides detailed error information."""
        try:
            from src.operators.quality.schema_validator import SchemaValidator

            expected_schema = {
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"},
                ]
            }

            operator = SchemaValidator(
                task_id="detailed_errors",
                table_name="test_table",
                expected_schema=expected_schema,
            )

            # Mock database with multiple issues
            with patch.object(operator, "get_table_schema") as mock_get_schema:
                mock_get_schema.return_value = [
                    {"column_name": "id", "data_type": "text"},  # Wrong type
                    {"column_name": "name", "data_type": "character varying"},
                    # Missing 'email' column
                    {"column_name": "extra_col", "data_type": "integer"},  # Extra column
                ]

                result = operator.execute(mock_context)
                assert result["passed"] is False
                # Should have detailed breakdown
                assert "missing_columns" in result or "type_mismatches" in result
                assert "errors" in result or "details" in result
        except ImportError:
            pytest.skip("SchemaValidator not implemented yet")
