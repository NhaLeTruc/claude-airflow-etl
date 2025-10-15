# Data Model: Apache Airflow ETL Demo Platform

**Date**: 2025-10-15
**Feature**: Apache Airflow ETL Demo Platform
**Branch**: `001-build-a-full`

## Overview

This document defines the data models for the Apache Airflow ETL Demo Platform, including DAG configuration schemas, mock data warehouse tables, data quality metadata, and operational entities.

---

## DAG Configuration Schema

### DAGConfiguration

JSON schema for dynamic DAG generation from configuration files.

**Purpose**: Define complete DAG specifications declaratively for the DAG factory.

**Attributes**:
- `dag_id` (string, required): Unique identifier following pattern `{domain}_{pipeline_name}_{version}`
  - Example: `sales_daily_aggregation_v1`
  - Validation: Must match regex `^[a-z][a-z0-9_]*_v[0-9]+$`
- `description` (string, optional): Human-readable DAG description
- `schedule` (string | null, optional): Cron expression or Airflow preset (`@daily`, `@hourly`, etc.)
  - Default: `null` (manual trigger only)
- `catchup` (boolean, optional): Whether to backfill missed runs
  - Default: `false`
- `max_active_runs` (integer, optional): Maximum concurrent runs
  - Default: `1`
  - Range: 1-10
- `tags` (array of strings, optional): DAG categorization tags
  - Example: `["beginner", "data-quality", "demo"]`
- `default_args` (object, optional): Default arguments for all tasks
  - `owner` (string): DAG owner email or username
  - `retries` (integer): Number of retry attempts (0-5)
  - `retry_delay` (integer): Seconds between retries (default: 300)
  - `retry_exponential_backoff` (boolean): Use exponential backoff (default: `true`)
  - `execution_timeout` (integer): Maximum task execution time in seconds
  - `email_on_failure` (boolean): Send email on task failure
  - `email_on_retry` (boolean): Send email on task retry
- `tasks` (array of TaskDefinition, required): List of tasks in DAG (minimum 1)

**Relationships**:
- Contains 1+ TaskDefinition objects
- Referenced by DAG execution history
- Validated against JSONSchema at runtime

**Validation Rules**:
- `dag_id` must be unique across all configurations
- Task IDs within `tasks` array must be unique
- Task dependencies must reference valid task IDs (no dangling references)
- Task dependency graph must be acyclic (no circular dependencies)
- At least one task must have no upstream dependencies (DAG entry point)

**State Transitions**:
1. **Created**: Configuration file added to `dags/config/`
2. **Validated**: Passes JSON schema validation
3. **Registered**: DAG factory instantiates DAG and registers with Airflow
4. **Active**: DAG visible in Airflow UI and schedulable
5. **Updated**: Configuration modified, triggers re-parsing
6. **Deprecated**: Configuration removed or `active: false` flag set

**Example**:
```json
{
  "dag_id": "sales_daily_etl_v1",
  "description": "Daily sales data extraction, transformation, and loading",
  "schedule": "@daily",
  "catchup": false,
  "max_active_runs": 1,
  "tags": ["sales", "daily", "production"],
  "default_args": {
    "owner": "data-engineering@example.com",
    "retries": 3,
    "retry_delay": 300,
    "retry_exponential_backoff": true,
    "execution_timeout": 3600,
    "email_on_failure": true
  },
  "tasks": [
    {
      "task_id": "extract_sales_data",
      "operator": "PostgresOperator",
      "parameters": {
        "sql": "SELECT * FROM raw.sales WHERE date = {{ ds }}",
        "postgres_conn_id": "warehouse"
      }
    },
    {
      "task_id": "validate_sales_data",
      "operator": "DataQualityOperator",
      "parameters": {
        "check_type": "schema_validation",
        "table": "raw.sales",
        "severity": "CRITICAL"
      },
      "upstream_tasks": ["extract_sales_data"]
    }
  ]
}
```

---

### TaskDefinition

Specification for individual tasks within a DAG configuration.

**Purpose**: Define task behavior, dependencies, and parameters declaratively.

**Attributes**:
- `task_id` (string, required): Unique task identifier within DAG
  - Validation: Must match regex `^[a-z][a-z0-9_]*$`
- `operator` (string, required): Operator class name or alias
  - Examples: `PostgresOperator`, `SparkStandaloneOperator`, `EmailNotificationOperator`
  - Must reference registered operator in operator registry
- `parameters` (object, required): Operator-specific parameters as key-value pairs
  - Structure varies by operator type
  - Validated against operator's parameter schema
- `upstream_tasks` (array of strings, optional): List of task IDs this task depends on
  - Default: `[]` (no dependencies)
  - Validation: All referenced task IDs must exist in same DAG
- `retries` (integer, optional): Override default retry count for this task
- `retry_delay` (integer, optional): Override default retry delay for this task
- `timeout` (integer, optional): Task-specific execution timeout in seconds

**Relationships**:
- Belongs to exactly one DAGConfiguration
- References zero or more other TaskDefinition objects (dependencies)
- Instantiated as Airflow BaseOperator subclass at runtime

**Validation Rules**:
- `task_id` must be unique within parent DAG configuration
- `operator` must exist in operator registry
- `parameters` must conform to operator's expected schema
- Dependency graph formed by `upstream_tasks` must be acyclic

**Example**:
```json
{
  "task_id": "submit_spark_aggregation",
  "operator": "SparkKubernetesOperator",
  "parameters": {
    "application": "/opt/spark/apps/sales_aggregation.py",
    "namespace": "spark-jobs",
    "spark_conf": {
      "spark.executor.instances": "2",
      "spark.executor.memory": "2g"
    }
  },
  "upstream_tasks": ["validate_sales_data"],
  "retries": 2,
  "timeout": 1800
}
```

---

## Mock Data Warehouse Schema

### Dimension Tables

#### DimCustomer

**Purpose**: Customer master data dimension table.

**Attributes**:
- `customer_id` (integer, primary key): Surrogate key
- `customer_key` (string, unique): Natural business key (e.g., "CUST-10001")
- `customer_name` (string): Full customer name
- `email` (string): Customer email address
- `country` (string): Customer country
- `segment` (string): Customer segment (e.g., "Enterprise", "SMB", "Consumer")
- `created_at` (timestamp): Record creation timestamp
- `updated_at` (timestamp): Last update timestamp

**Validation Rules**:
- `customer_key` must be unique and not null
- `email` must be valid email format
- `segment` must be one of predefined values

**Sample Data Generation**:
```python
from faker import Faker
fake = Faker()

{
    "customer_id": 1,
    "customer_key": f"CUST-{10000 + i}",
    "customer_name": fake.name(),
    "email": fake.email(),
    "country": fake.country(),
    "segment": random.choice(["Enterprise", "SMB", "Consumer"]),
    "created_at": fake.date_time_between(start_date="-2y"),
    "updated_at": datetime.now()
}
```

---

#### DimProduct

**Purpose**: Product master data dimension table.

**Attributes**:
- `product_id` (integer, primary key): Surrogate key
- `product_key` (string, unique): SKU or product code (e.g., "PROD-ABC123")
- `product_name` (string): Product name
- `category` (string): Product category (e.g., "Electronics", "Clothing")
- `subcategory` (string): Product subcategory
- `unit_price` (decimal): Standard unit price
- `created_at` (timestamp): Record creation timestamp
- `updated_at` (timestamp): Last update timestamp

**Validation Rules**:
- `product_key` must be unique and not null
- `unit_price` must be positive

**Sample Data Generation**:
```python
{
    "product_id": 1,
    "product_key": f"PROD-{fake.bothify(text='???###')}",
    "product_name": fake.catch_phrase(),
    "category": random.choice(["Electronics", "Clothing", "Food", "Books"]),
    "subcategory": fake.word(),
    "unit_price": round(random.uniform(5.0, 500.0), 2),
    "created_at": fake.date_time_between(start_date="-2y"),
    "updated_at": datetime.now()
}
```

---

#### DimDate

**Purpose**: Time dimension table for date-based analytics.

**Attributes**:
- `date_id` (integer, primary key): YYYYMMDD format (e.g., 20250115)
- `date` (date, unique): Calendar date
- `year` (integer): Year (e.g., 2025)
- `quarter` (integer): Quarter (1-4)
- `month` (integer): Month (1-12)
- `month_name` (string): Month name (e.g., "January")
- `week` (integer): ISO week number (1-53)
- `day_of_week` (integer): Day of week (1=Monday, 7=Sunday)
- `day_name` (string): Day name (e.g., "Monday")
- `is_weekend` (boolean): True if Saturday/Sunday
- `is_holiday` (boolean): True if public holiday

**Validation Rules**:
- Must contain continuous date range (no gaps)
- Typically pre-populated for 5+ year range

---

### Fact Tables

#### FactSales

**Purpose**: Sales transaction fact table.

**Attributes**:
- `sale_id` (bigint, primary key): Surrogate key
- `transaction_id` (string, unique): Business transaction identifier
- `customer_id` (integer, foreign key): Reference to DimCustomer
- `product_id` (integer, foreign key): Reference to DimProduct
- `sale_date_id` (integer, foreign key): Reference to DimDate
- `quantity` (integer): Number of units sold
- `unit_price` (decimal): Price per unit at time of sale
- `discount` (decimal): Discount amount applied
- `total_amount` (decimal): Total sale amount (calculated: quantity * unit_price - discount)
- `created_at` (timestamp): Record creation timestamp

**Relationships**:
- Many-to-one with DimCustomer via `customer_id`
- Many-to-one with DimProduct via `product_id`
- Many-to-one with DimDate via `sale_date_id`

**Validation Rules**:
- `quantity` must be positive
- `unit_price` must be positive
- `discount` must be non-negative and less than or equal to `quantity * unit_price`
- `total_amount` must equal `quantity * unit_price - discount`

**Intentional Quality Issues** (for testing):
- 2% rows with null `customer_id` (orphaned records)
- 1% rows with negative `quantity` (data entry errors)
- 0.5% rows with `total_amount` != calculated value (calculation errors)
- 3% duplicate `transaction_id` values (duplicate detection testing)

**Sample Data Generation**:
```python
{
    "sale_id": i,
    "transaction_id": f"TXN-{fake.bothify(text='########')}",
    "customer_id": random.randint(1, 1000),
    "product_id": random.randint(1, 500),
    "sale_date_id": int(fake.date_between(start_date="-1y").strftime("%Y%m%d")),
    "quantity": random.randint(1, 10),
    "unit_price": round(random.uniform(10.0, 300.0), 2),
    "discount": round(random.uniform(0.0, 50.0), 2),
    "total_amount": # calculated
    "created_at": datetime.now()
}
```

---

### Staging Tables

#### StagingSales

**Purpose**: Landing zone for raw sales data before transformation.

**Attributes**:
- Same structure as FactSales plus:
- `source_system` (string): Origin system identifier
- `load_timestamp` (timestamp): When record was loaded into staging
- `is_processed` (boolean): Whether record has been validated and moved to fact table

**Purpose in ETL**:
- Demonstrates incremental load patterns
- Enables data quality validation before promoting to fact table
- Supports idempotent DAG execution (can re-process same staging data)

---

## Data Quality Metadata

### QualityCheckResult

**Purpose**: Store historical results of data quality validations.

**Attributes**:
- `check_id` (bigint, primary key): Surrogate key
- `dag_id` (string): DAG that ran the quality check
- `task_id` (string): Task that ran the quality check
- `execution_date` (timestamp): DAG execution date
- `table_name` (string): Target table checked
- `check_type` (string): Type of check (e.g., "schema_validation", "completeness_check")
- `severity` (string): Severity level ("CRITICAL", "WARNING", "INFO")
- `passed` (boolean): Whether check passed
- `rows_checked` (integer): Number of rows evaluated
- `rows_failed` (integer): Number of rows failing validation
- `failure_rate` (decimal): Percentage of failed rows
- `threshold` (decimal): Configured threshold for this check
- `error_message` (text, nullable): Error details if check failed
- `metadata` (jsonb): Additional check-specific metadata
- `created_at` (timestamp): When check ran

**Relationships**:
- Associated with specific DAG run (via dag_id + execution_date)
- Referenced by notification tasks for alerting

**Validation Rules**:
- `severity` must be one of: "CRITICAL", "WARNING", "INFO"
- `failure_rate` = (`rows_failed` / `rows_checked`) * 100
- `passed` = `true` if `failure_rate` <= `threshold`, else `false`

**Example**:
```json
{
  "check_id": 12345,
  "dag_id": "sales_daily_etl_v1",
  "task_id": "validate_sales_schema",
  "execution_date": "2025-10-15T00:00:00Z",
  "table_name": "staging.sales",
  "check_type": "schema_validation",
  "severity": "CRITICAL",
  "passed": false,
  "rows_checked": 10000,
  "rows_failed": 5,
  "failure_rate": 0.05,
  "threshold": 0.0,
  "error_message": "5 rows missing required column 'customer_id'",
  "metadata": {
    "expected_columns": ["transaction_id", "customer_id", "product_id", ...],
    "missing_columns": [],
    "extra_columns": [],
    "invalid_rows": [123, 456, 789, 1011, 1213]
  },
  "created_at": "2025-10-15T01:23:45Z"
}
```

---

### ExpectationSuite

**Purpose**: Define Great Expectations validation suites for datasets.

**Attributes**:
- `suite_id` (integer, primary key): Surrogate key
- `suite_name` (string, unique): Expectation suite name
- `table_name` (string): Target table for expectations
- `expectations` (jsonb): Great Expectations expectation configuration
- `version` (integer): Suite version number (for change tracking)
- `created_at` (timestamp): When suite was created
- `updated_at` (timestamp): Last modification

**Relationships**:
- One suite per table (can have multiple versions)
- Referenced by DataQualityOperator tasks

**Example Expectation Configuration**:
```json
{
  "suite_id": 1,
  "suite_name": "sales_fact_table_quality",
  "table_name": "fact_sales",
  "expectations": {
    "expectations": [
      {
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {
          "column_list": ["sale_id", "transaction_id", "customer_id", ...]
        }
      },
      {
        "expectation_type": "expect_column_values_to_be_unique",
        "kwargs": {
          "column": "transaction_id"
        }
      },
      {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {
          "column": "quantity",
          "min_value": 1,
          "max_value": 1000
        }
      }
    ]
  },
  "version": 1
}
```

---

## Operational Entities

### SparkJobSubmission

**Purpose**: Track Spark job submissions and execution metadata.

**Attributes**:
- `job_id` (bigint, primary key): Surrogate key
- `dag_id` (string): DAG that submitted the job
- `task_id` (string): Task that submitted the job
- `execution_date` (timestamp): DAG execution date
- `cluster_type` (string): Cluster type ("standalone", "yarn", "kubernetes")
- `application_path` (string): Path to Spark application JAR/Python file
- `spark_job_id` (string): Spark-assigned job ID
- `status` (string): Job status ("SUBMITTED", "RUNNING", "SUCCESS", "FAILED")
- `submitted_at` (timestamp): When job was submitted
- `started_at` (timestamp, nullable): When job started running
- `completed_at` (timestamp, nullable): When job completed
- `duration_seconds` (integer, nullable): Total execution time
- `error_message` (text, nullable): Error details if job failed
- `logs_url` (string, nullable): URL to Spark UI or logs

**State Transitions**:
1. SUBMITTED → RUNNING → SUCCESS/FAILED
2. Transition triggers: Spark cluster status API polling

**Validation Rules**:
- `status` must be one of: "SUBMITTED", "RUNNING", "SUCCESS", "FAILED"
- `completed_at` must be after `started_at`
- `duration_seconds` = `completed_at` - `started_at`

---

### NotificationLog

**Purpose**: Audit trail for notification deliveries.

**Attributes**:
- `notification_id` (bigint, primary key): Surrogate key
- `dag_id` (string): DAG that triggered notification
- `task_id` (string): Task that triggered notification
- `execution_date` (timestamp): DAG execution date
- `channel` (string): Notification channel ("email", "teams", "telegram")
- `recipients` (jsonb): List of recipients or webhook URLs
- `subject` (string): Notification subject/title
- `message` (text): Notification body
- `status` (string): Delivery status ("SENT", "FAILED", "RETRYING")
- `sent_at` (timestamp): When notification was sent
- `error_message` (text, nullable): Error details if delivery failed
- `retry_count` (integer): Number of retry attempts

**Validation Rules**:
- `channel` must be one of: "email", "teams", "telegram"
- `status` must be one of: "SENT", "FAILED", "RETRYING"
- `retry_count` <= 3 (max retries as per operator implementation)

---

## Entity Relationship Diagram

```
┌─────────────────┐         ┌─────────────────┐
│ DAGConfiguration│1       *│ TaskDefinition  │
│─────────────────│◄────────│─────────────────│
│ dag_id (PK)     │         │ task_id (PK)    │
│ description     │         │ operator        │
│ schedule        │         │ parameters      │
│ tasks[]         │         │ upstream_tasks[]│
└─────────────────┘         └─────────────────┘
                                     │
                                     │ executes
                                     ▼
┌─────────────────┐         ┌──────────────────┐
│ DimCustomer     │         │ QualityCheckResult│
│─────────────────│         │──────────────────│
│ customer_id (PK)│         │ check_id (PK)    │
│ customer_key    │         │ dag_id           │
│ customer_name   │         │ task_id          │
└────────┬────────┘         │ table_name       │
         │                  │ check_type       │
         │ FK               │ severity         │
         ▼                  │ passed           │
┌─────────────────┐         └──────────────────┘
│ FactSales       │
│─────────────────│         ┌──────────────────┐
│ sale_id (PK)    │         │ SparkJobSubmission│
│ customer_id (FK)│         │──────────────────│
│ product_id (FK) │         │ job_id (PK)      │
│ sale_date_id(FK)│         │ dag_id           │
│ quantity        │         │ cluster_type     │
│ total_amount    │         │ status           │
└────────┬────────┘         └──────────────────┘
         │
         │ FK               ┌──────────────────┐
         ▼                  │ NotificationLog  │
┌─────────────────┐         │──────────────────│
│ DimProduct      │         │ notification_id  │
│─────────────────│         │ dag_id           │
│ product_id (PK) │         │ channel          │
│ product_key     │         │ status           │
│ product_name    │         └──────────────────┘
└────────┬────────┘
         │
         │ FK
         ▼
┌─────────────────┐
│ DimDate         │
│─────────────────│
│ date_id (PK)    │
│ date            │
│ year, quarter   │
└─────────────────┘
```

---

## Indexes and Performance

### Recommended Indexes

**FactSales**:
- Primary key: `sale_id`
- Foreign keys: `customer_id`, `product_id`, `sale_date_id`
- Query index: `(sale_date_id, customer_id)` for time-series customer analysis
- Uniqueness: `transaction_id`

**QualityCheckResult**:
- Primary key: `check_id`
- Query index: `(dag_id, execution_date)` for DAG run history
- Query index: `(table_name, created_at)` for table quality trends

**SparkJobSubmission**:
- Primary key: `job_id`
- Query index: `(dag_id, execution_date)` for DAG run correlation
- Query index: `(status, submitted_at)` for active job monitoring

**NotificationLog**:
- Primary key: `notification_id`
- Query index: `(dag_id, execution_date)` for DAG run notifications
- Query index: `(channel, status)` for delivery monitoring

---

## Data Volume Estimates

**For Demo/Test Purposes**:
- DimCustomer: 1,000 rows
- DimProduct: 500 rows
- DimDate: 1,825 rows (5 years)
- FactSales: 100,000 rows (initial seed), grows by ~1,000/day in examples
- StagingSales: 1,000-5,000 rows per DAG run
- QualityCheckResult: 50-100 rows per DAG run
- SparkJobSubmission: 10-20 rows per DAG run (Spark examples)
- NotificationLog: 20-50 rows per DAG run

**Storage**: Estimated < 100 MB for full dataset (easily fits in PostgreSQL container).

---

## Summary

This data model supports:
1. Dynamic DAG generation via JSON configuration
2. Realistic data warehouse with star schema (dimensions + facts)
3. Comprehensive data quality tracking and historical analysis
4. Operational metadata for Spark jobs and notifications
5. Educational examples with intentional quality issues for testing

All entities are designed for:
- Testability (mocked data generation)
- Observability (audit trails and metadata)
- Scalability (indexed for common query patterns)
- Constitutional compliance (quality checks, lineage, logging)
