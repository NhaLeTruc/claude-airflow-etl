# Research: Apache Airflow ETL Demo Platform

**Date**: 2025-10-15
**Feature**: Apache Airflow ETL Demo Platform
**Branch**: `001-build-a-full`

## Technology Stack Decisions

### Core Platform

#### Decision: Apache Airflow 2.8+

**Rationale**:
- Latest stable version (2.8.x as of Q4 2024) with modern features and security patches
- Dynamic DAG generation support via DAG factories (stable since 2.0+)
- Improved scheduler performance and reduced DAG parsing overhead
- Built-in support for custom operators, hooks, and plugins
- Mature ecosystem with extensive community support and documentation

**Alternatives Considered**:
- **Airflow 2.7**: Previous stable but missing recent performance improvements
- **Airflow 3.0**: Not yet stable (in development), too bleeding edge for demo
- **Prefect 2.x**: Different paradigm (Python-first vs DAG-first), less aligned with requirements
- **Dagster**: Asset-centric model doesn't match DAG-first constitutional principle

**Implementation Notes**:
- Use Airflow's CeleryExecutor for production-like execution environment
- Enable DAG serialization for faster parsing
- Configure LocalExecutor for Docker Compose local development
- Leverage Airflow's built-in logging and monitoring capabilities

---

### Language and Runtime

#### Decision: Python 3.11

**Rationale**:
- Latest stable Python with enhanced type hints (PEP 646, 673, 675)
- 10-60% performance improvements over Python 3.10 (CPython optimizations)
- Better error messages for debugging
- Full compatibility with Airflow 2.8+ and all required dependencies
- Long-term support ensuring project remains maintainable

**Alternatives Considered**:
- **Python 3.10**: Widely used but missing 3.11 performance gains
- **Python 3.12**: Too new, potential compatibility issues with some libraries
- **Python 3.9**: Older LTS but lacks modern type system features

**Implementation Notes**:
- Use `pyproject.toml` for modern Python project configuration
- Leverage structural pattern matching (PEP 634) for cleaner code
- Enable strict type checking with mypy using 3.11 features

---

### Data Storage

#### Decision: PostgreSQL 15+

**Rationale**:
- Required for Airflow metadata database (officially supported backend)
- Serves dual purpose as mock data warehouse (reduces container count)
- JSONB support for storing quality check results and metadata
- Excellent Docker support with official images
- Widely known by data engineers (educational value)
- Robust transaction support for data quality testing

**Alternatives Considered**:
- **MySQL**: Supported by Airflow but less feature-rich for analytics use cases
- **SQLite**: Not supported for production Airflow (concurrent access issues)
- **Separate warehouse DB**: Adds complexity without educational benefit

**Implementation Notes**:
- Use PostgreSQL 15 Docker image (latest stable)
- Create separate schemas for Airflow metadata vs warehouse data
- Implement connection pooling for efficient resource usage
- Use partitioning for warehouse fact tables to demonstrate ETL patterns

---

### Testing Framework

#### Decision: pytest 8.0+ with pytest-cov, pytest-docker, pytest-mock

**Rationale**:
- Industry standard for Python testing with excellent plugin ecosystem
- `pytest-cov`: Built-in coverage reporting (required for 80%+ coverage goal)
- `pytest-docker`: Enables integration tests against containerized services
- `pytest-mock`: Simplifies mocking external dependencies (Spark, notifications)
- Fixtures provide clean test data management
- Parameterized tests reduce code duplication for similar test cases

**Alternatives Considered**:
- **unittest**: Built-in but more verbose and less feature-rich
- **nose2**: Less active development, smaller ecosystem
- **tox**: Adds matrix testing complexity not needed for demo scope

**Implementation Notes**:
- Use `conftest.py` for shared fixtures (mock DAG configs, warehouse connections)
- Implement custom pytest marks: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`
- Generate HTML coverage reports for CI/CD visibility
- Use `pytest-xdist` for parallel test execution (faster CI)

---

### Code Quality Tools

#### Decision: ruff (linting), black (formatting), mypy (type checking)

**Rationale**:
- **ruff**: 10-100x faster than flake8/pylint, written in Rust, actively maintained
  - Replaces flake8, isort, pydocstyle in single tool
  - Autofix capabilities reduce manual corrections
  - Growing rule set covers security, performance, and style
- **black**: Opinionated formatter eliminates style debates
  - Consistent formatting across codebase
  - Integration with most editors and CI systems
  - 100 character line length for readability in code reviews
- **mypy**: Static type checker catches bugs before runtime
  - Required for constitutional type hint mandate
  - Configurable strictness levels
  - Excellent IDE integration

**Alternatives Considered**:
- **flake8 + plugins**: Slower, requires multiple tools for ruff's functionality
- **pylint**: Slower than ruff, more opinionated (sometimes too strict)
- **pyright**: Faster than mypy but less mature Python support

**Implementation Notes**:
- Configure tools in `pyproject.toml` for centralized configuration
- Set ruff to maximum strictness compatible with Airflow codebase patterns
- Enable mypy strict mode with gradual typing for migration path
- Use pre-commit hooks to enforce checks before commits

---

### Data Quality Framework

#### Decision: Great Expectations 0.18+

**Rationale**:
- Purpose-built for data quality validation in Python
- Airflow integration via GreatExpectationsOperator
- Declarative expectation suites match JSON config philosophy
- Generates human-readable data quality reports (HTML, JSON)
- Supports all required check types (schema, completeness, freshness, uniqueness, null rate)
- Version control for expectations aligns with constitutional requirement

**Alternatives Considered**:
- **Custom validators**: More flexible but requires extensive development
- **deequ**: Scala-based (requires JVM), less Python-native
- **soda-core**: Newer, less mature ecosystem

**Implementation Notes**:
- Create expectation suites for each warehouse table/dataset
- Store expectations in version control alongside DAG configs
- Integrate validation results with Airflow XCom for downstream tasks
- Configure severity levels (CRITICAL/WARNING/INFO) via custom validation actions

---

### Spark Integration

#### Decision: PySpark 3.5+ with multiple cluster simulation

**Rationale**:
- PySpark provides Python API for Spark job submission and monitoring
- Version 3.5 includes security patches and performance improvements
- Simulated clusters avoid infrastructure complexity while demonstrating concepts:
  - **Standalone**: Simple single-container Spark master/worker
  - **YARN**: Mocked via environment variables and job submission patterns
  - **Kubernetes**: Simulated using K8s-in-Docker (kind) for demo purposes

**Alternatives Considered**:
- **Real clusters**: Too heavy for local development, requires infrastructure
- **Spark 3.4**: Older version, missing recent improvements
- **Databricks SDK**: Cloud-specific, not self-contained demo

**Implementation Notes**:
- Create lightweight Spark jobs (word count, simple aggregations) for fast execution
- Use Airflow's SparkSubmitOperator as base class for custom operators
- Implement cluster-specific configuration in operator parameters
- Mock Spark UI for job monitoring demonstrations

---

### Notification Integrations

#### Decision: Native Python libraries - smtplib, requests, python-telegram-bot

**Rationale**:
- **Email (smtplib)**: Built-in Python library, no external dependencies
- **MS Teams (requests)**: Webhook-based, simple POST requests with message cards
- **Telegram (python-telegram-bot)**: Official library with excellent documentation
- All support async operations for non-blocking notifications
- Easy to mock for testing (no real API calls needed in tests)

**Alternatives Considered**:
- **Airflow EmailOperator**: Limited customization, doesn't demonstrate custom operator pattern
- **Slack**: Similar to Teams but Teams demonstrates enterprise integration
- **PagerDuty/Opsgenie**: Incident management focus, overkill for demo

**Implementation Notes**:
- Implement base `NotificationOperator` class for common retry/timeout logic
- Use Jinja2 templates for message formatting with DAG context
- Graceful degradation: log notification failures without failing DAG tasks
- Mock SMTP server and webhook endpoints in integration tests

---

### Containerization and Orchestration

#### Decision: Docker Compose for local development

**Rationale**:
- Simplest multi-container orchestration for local development
- Single `docker-compose.yml` defines entire stack (Airflow, PostgreSQL, Spark, warehouse)
- Fast startup time (target: under 3 minutes)
- Volume mounts enable live code reloading during development
- Widely known by developers (low barrier to entry)

**Alternatives Considered**:
- **Minikube/kind**: Kubernetes adds complexity for local dev
- **Podman Compose**: Less widespread adoption than Docker
- **Manual containers**: Harder to orchestrate multiple services

**Implementation Notes**:
- Use official Docker images where possible (postgres:15, apache/airflow:2.8)
- Create custom Dockerfiles only for Spark clusters and extended Airflow image
- Implement health checks for all services to ensure proper startup order
- Use named volumes for data persistence across restarts

---

### CI/CD Pipeline

#### Decision: GitHub Actions

**Rationale**:
- Native GitHub integration (repository already on GitHub per requirements)
- Free for public repositories with generous runner minutes
- Matrix builds for testing against multiple Python/Airflow versions
- Secrets management for deployment credentials
- Extensive marketplace for pre-built actions (Docker, pytest, coverage upload)
- Workflow as code (YAML) versioned with project

**Alternatives Considered**:
- **GitLab CI**: Not available if repository is on GitHub
- **Jenkins**: Requires self-hosted infrastructure (more complex)
- **CircleCI**: External service, costs for private repos
- **Travis CI**: Less active development, declining adoption

**Implementation Notes**:
- Three workflow files: `ci.yml` (lint, test, validate), `cd-staging.yml`, `cd-production.yml`
- Use matrix strategy to test Python 3.11 with Airflow 2.8.x
- Cache pip dependencies for faster builds
- Require all checks pass before merge (branch protection rules)
- Generate and upload coverage reports to codecov or similar

---

### Build and Dependency Management

#### Decision: pip-tools for dependency pinning, setuptools for packaging

**Rationale**:
- **pip-tools**: Generates `requirements.txt` from `requirements.in` with exact versions
  - Ensures reproducible builds (constitutional requirement)
  - Separates direct dependencies from transitive ones
  - `pip-compile` handles version resolution conflicts
- **setuptools**: Mature, standard Python packaging tool
  - Required for installing project as editable package (`pip install -e .`)
  - Enables clean imports across project structure
  - Compatible with all Python environments

**Alternatives Considered**:
- **Poetry**: Adds complexity, not all teams use it, learning curve
- **pipenv**: Slower than pip-tools, less transparent lock files
- **conda**: Overkill for Python-only project, slower environment creation

**Implementation Notes**:
- Maintain `requirements.in` (direct deps) and `requirements-dev.in` (test/dev tools)
- Pin to specific versions for stability: `apache-airflow==2.8.1` not `>=2.8`
- Run `pip-compile` in CI to ensure requirements.txt is up-to-date
- Use `pyproject.toml` for tool configuration, metadata, and build settings

---

### Mock Data Generation

#### Decision: Faker library for synthetic data

**Rationale**:
- Purpose-built for generating realistic fake data
- Supports multiple data types (names, addresses, dates, numbers, text)
- Localization for international data scenarios
- Deterministic seeding for reproducible test data
- Lightweight and fast for generating large datasets

**Alternatives Considered**:
- **Hand-written fixtures**: Time-consuming, less realistic
- **Mimesis**: Similar to Faker but less widely known
- **Real anonymized data**: Violates "no real data" requirement, privacy concerns

**Implementation Notes**:
- Create data generator utilities in `src/utils/data_generator.py`
- Seed generators with fixed values for deterministic test fixtures
- Generate warehouse dimension tables (customers, products, time) and fact tables (sales, orders)
- Inject intentional quality issues for validation testing (nulls, duplicates, schema violations)

---

### Documentation

#### Decision: Markdown with MkDocs for generated docs (optional enhancement)

**Rationale**:
- Markdown: Universal format, readable in raw form, renders well on GitHub
- MkDocs (optional): Generates searchable static site from markdown
- Inline DAG documentation using docstrings extracted by Airflow UI
- Architecture diagrams using Mermaid.js (GitHub support)

**Alternatives Considered**:
- **Sphinx**: More complex setup, RST syntax less readable
- **Plain README**: Insufficient for extensive documentation needs
- **Wiki**: Separate from code, harder to version control

**Implementation Notes**:
- Required docs: README.md, architecture.md, dag_configuration.md, operator_guide.md, development.md
- Use ADR (Architecture Decision Records) format for major technical decisions
- Include code examples and JSON config snippets in documentation
- Generate API reference from docstrings as part of CI build

---

## Best Practices Research

### Dynamic DAG Generation Patterns

**Research Findings**:
- Airflow 2.x supports DAG factories via global scope execution
- JSON configs should be validated against JSON Schema before DAG instantiation
- Glob patterns efficiently discover config files: `Path("dags/config/*.json").glob()`
- Use DAG ID from config, fallback to filename hash for uniqueness
- Implement caching to avoid re-parsing unchanged configs (performance optimization)

**Recommended Pattern**:
```python
# dags/factory/dag_builder.py
def create_dag_from_config(config_path: Path) -> DAG:
    """Generate Airflow DAG from JSON configuration."""
    config = load_and_validate_config(config_path)
    dag = DAG(
        dag_id=config["dag_id"],
        schedule=config.get("schedule", None),
        default_args=config.get("default_args", {}),
        catchup=config.get("catchup", False),
    )
    # Build tasks from config["tasks"] list
    # Return fully constructed DAG
    return dag

# Discover and instantiate all configured DAGs
for config_file in Path("dags/config").glob("*.json"):
    globals()[f"dag_{config_file.stem}"] = create_dag_from_config(config_file)
```

---

### Airflow Testing Best Practices

**Research Findings**:
- Use `airflow.models.DagBag` to test DAG parsing without execution
- `airflow.utils.dag_processing.list_py_file_paths` helps discover DAG files
- Integration tests should use `airflow tasks test` command (skips dependencies)
- Mock Airflow context with `get_current_context()` for operator testing
- Use `pytest-docker-compose` to spin up Airflow environment for integration tests

**Recommended Test Structure**:
```python
# tests/integration/test_dag_parsing.py
def test_all_dags_parse_without_errors():
    """Verify all DAG files can be parsed by Airflow."""
    from airflow.models import DagBag
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"Errors: {dag_bag.import_errors}"
    assert len(dag_bag.dags) >= 12, "Should have at least 12 example DAGs"
```

---

### Spark Operator Implementation

**Research Findings**:
- Airflow's `BaseOperator` provides retry, timeout, and callback infrastructure
- SparkSubmitOperator handles job submission but lacks multi-cluster abstraction
- Custom operators should inherit from `BaseOperator` and implement `execute()` method
- Use Airflow connections to store cluster credentials and endpoints
- Poll job status using Spark REST API or cluster-specific monitoring

**Recommended Pattern**:
```python
class BaseSparkSubmitOperator(BaseOperator):
    """Base class for Spark job submission across cluster types."""

    def __init__(self, application: str, cluster_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.application = application
        self.cluster_conn_id = cluster_conn_id

    def execute(self, context):
        hook = self.get_hook()  # Cluster-specific hook
        job_id = hook.submit_job(self.application)
        hook.poll_job_status(job_id)
        return hook.get_job_result(job_id)
```

---

### Data Quality Implementation

**Research Findings**:
- Great Expectations integrates with Airflow via custom operator or PythonOperator
- Expectation suites should be versioned alongside DAG code
- Validation results can be passed via XCom to downstream notification tasks
- Configure validation actions for each severity level (CRITICAL fails pipeline, WARNING logs)
- Use checkpoint pattern to group expectations and validation logic

**Recommended Pattern**:
```python
# src/operators/quality/base_quality_operator.py
class DataQualityOperator(BaseOperator):
    """Base operator for data quality checks with severity levels."""

    def __init__(self, check_type: str, severity: Literal["CRITICAL", "WARNING", "INFO"], **kwargs):
        super().__init__(**kwargs)
        self.check_type = check_type
        self.severity = severity

    def execute(self, context):
        result = self.run_check()
        if not result.success and self.severity == "CRITICAL":
            raise AirflowException(f"Critical quality check failed: {result.message}")
        # Log and return result for downstream tasks
        return result
```

---

## Architecture Decisions

### Decision: Separate DAG factory from example DAGs

**Rationale**:
- Factory provides dynamic generation, examples show hand-coded DAGs
- Educational value: users see both approaches
- Examples demonstrate patterns that can be converted to JSON configs

### Decision: Custom operators over PythonOperator with callables

**Rationale**:
- Better encapsulation and testability
- Reusable across multiple DAGs
- Demonstrates Airflow extensibility patterns
- Clearer parameter validation and type hints

### Decision: PostgreSQL as both metadata DB and mock warehouse

**Rationale**:
- Reduces container count and startup time
- Schema separation provides logical isolation
- Realistic: many organizations use PostgreSQL for both purposes
- Simplifies connection management in DAGs

### Decision: Three-tier testing (unit, integration, contract)

**Rationale**:
- Unit tests: Fast feedback for operator logic
- Integration tests: Validate DAG execution end-to-end
- Contract tests: Ensure JSON schemas remain backward compatible
- Aligns with constitutional testing requirements

---

## Open Questions Resolved

**Q: Should we use Airflow 2.8 or wait for 3.0?**
A: Use 2.8 (latest stable). Version 3.0 is in development and not production-ready.

**Q: How to simulate multiple Spark cluster types without infrastructure overhead?**
A: Use mocked cluster environments with environment variable-based configuration. Demonstrate submission patterns without requiring actual YARN/K8s infrastructure.

**Q: Should we use Airflow's built-in EmailOperator or create custom?**
A: Create custom to demonstrate operator development patterns and enable consistent notification interface across channels.

**Q: How to ensure 80%+ test coverage without over-testing?**
A: Focus coverage on business logic (operators, quality checks, DAG factory). Exclude boilerplate and configuration files. Use `pytest-cov --cov-report=term-missing` to identify gaps.

**Q: Should DAG configs support environment-specific overrides?**
A: Yes, via Jinja2 templating in JSON (Airflow supports this pattern). Enables dev/staging/prod customization.

---

## Summary

All technology choices prioritize:
1. **Stability**: Latest stable versions, avoiding bleeding edge
2. **Minimalism**: Fewest dependencies to achieve requirements
3. **Education**: Tools and patterns widely adopted in industry
4. **Testability**: First-class support for comprehensive testing
5. **Constitutional Compliance**: Every choice supports the five core principles

Next phase: Translate these decisions into data models, API contracts, and quickstart guide.
