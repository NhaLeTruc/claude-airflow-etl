# Test Fixtures

This directory contains mock data and configurations for testing.

## Structure

- `mock_configs/` - Mock DAG configuration JSON files
  - `simple_etl.json` - Valid simple ETL pipeline configuration
  - `invalid_dag.json` - Invalid configuration for negative testing

- `mock_datasets/` - Mock dataset files for testing operators
  - `customers_sample.json` - Sample customer records (with null email)
  - `sales_with_issues.json` - Sample sales with data quality issues (null customer_id, negative quantity, duplicate transaction_id)

## Usage

These fixtures are automatically available via pytest fixtures in `conftest.py`:

```python
def test_load_dag_config(mock_dag_configs_dir):
    config_path = mock_dag_configs_dir / "simple_etl.json"
    # Use config_path in test
```

## Adding New Fixtures

1. Add new JSON files to appropriate subdirectory
2. Update this README with description
3. Add pytest fixture in `conftest.py` if needed
