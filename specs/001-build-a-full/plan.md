# Implementation Plan: Apache Airflow ETL Demo Platform

**Branch**: `001-build-a-full` | **Date**: 2025-10-15 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-build-a-full/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Build a comprehensive Apache Airflow ETL demonstration platform that showcases enterprise-grade data pipeline orchestration through dynamic DAG generation from JSON configurations. The platform demonstrates Airflow's capabilities for handling complex ETL workflows with failure resilience, multi-cluster Spark job orchestration, multi-channel notifications, and comprehensive data quality validation. The system serves as both a learning resource with 12+ example DAGs and a production-ready reference implementation with full test coverage, local Docker Compose development environment, and automated CI/CD pipeline using GitHub Actions.

## Technical Context

**Language/Version**: Python 3.11 (latest stable with modern type hints and performance improvements)
**Primary Dependencies**: Apache Airflow 2.8+ (latest stable), PostgreSQL 15+ (mock data warehouse), Great Expectations 0.18+ (data quality), PySpark 3.5+ (Spark integration)
**Storage**: PostgreSQL 15+ for Airflow metadata and mock data warehouse
**Testing**: pytest 8.0+ with pytest-cov for coverage, pytest-docker for integration tests
**Target Platform**: Linux containers (Docker) for local dev, deployable to Kubernetes or cloud-managed Airflow
**Project Type**: Single Python project with DAG-centric structure
**Performance Goals**: DAG parsing under 30 seconds for all 12 examples, individual DAG execution under 5 minutes, Docker Compose startup under 3 minutes
**Constraints**: Minimal external dependencies (no cloud services required), mock data only (no real external systems), 80%+ test coverage mandatory
**Scale/Scope**: 12+ example DAGs, 5+ custom operators, 3 Spark cluster integrations, 3 notification channels, full CI/CD pipeline

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I: DAG-First Development ✅ PASS

- **Requirement**: Every ETL workflow MUST be implemented as an Airflow DAG with clear naming convention `{domain}_{pipeline_name}_{version}`
- **Compliance**: Dynamic DAG factory generates DAGs from JSON configs with standardized naming. All 12 example DAGs follow convention.
- **Evidence**: FR-001 through FR-006 mandate DAG generation, validation, and refresh. User Story 1 is dedicated to DAG-first architecture.

### Principle II: Test-Driven Data Engineering (NON-NEGOTIABLE) ✅ PASS

- **Requirement**: Multi-layer testing (unit, integration, data validation, DAG validation) with tests written BEFORE implementation
- **Compliance**: FR-042 through FR-046 mandate comprehensive testing. Success criterion SC-005 requires integration test suite with 100% pass rate. SC-010 requires 80%+ coverage.
- **Evidence**: User Story 6 mandates Docker Compose for integration testing. Spec explicitly requires all code tested with mocked data.

### Principle III: Data Quality Assurance (NON-NEGOTIABLE) ✅ PASS

- **Requirement**: Explicit quality checks at ingestion, transformation, and output with configurable thresholds
- **Compliance**: FR-047 through FR-050 mandate reusable quality operators with severity levels (CRITICAL/WARNING/INFO). Quality checks halt pipelines on CRITICAL failures.
- **Evidence**: Example DAG gallery (User Story 5) includes data quality validation patterns. FR-026 mandates quality validation examples.

### Principle IV: Code Quality and Maintainability ✅ PASS

- **Requirement**: Type hints, ruff linting, black formatting (100 char), complexity ≤10, docstrings, pinned dependencies
- **Compliance**: FR-036 mandates CI pipeline with ruff, black, mypy. FR-037 enforces zero linting errors. Constitution principle explicitly referenced in user input.
- **Evidence**: User Story 7 mandates automated linting/formatting checks in CI/CD pipeline before deployment.

### Principle V: Observability and Debugging ✅ PASS

- **Requirement**: Structured logging with context, duration tracking, data lineage, failure context, minimal XCom, explicit sensor timeouts
- **Compliance**: FR-012 mandates structured error logging with execution context. FR-008 mandates timeout policies. Notification system (FR-019 through FR-024) provides alerting.
- **Evidence**: Multi-channel notification system (User Story 4) ensures operational visibility. Example DAGs demonstrate logging best practices.

### Data Quality Standards ✅ PASS

- **Requirement**: Minimum quality checks (schema, completeness, freshness, uniqueness, null rate) with standardized reporting
- **Compliance**: FR-047 mandates all five check types. FR-048 mandates severity levels. FR-049 mandates pipeline halt on CRITICAL failures.
- **Evidence**: Success criterion SC-007 requires 100% detection of injected data anomalies in test scenarios.

### Development Workflow ✅ PASS

- **Requirement**: Local testability with pytest, mock data fixtures, Docker Compose, README documentation
- **Compliance**: FR-030 through FR-035 mandate complete Docker Compose environment with mock data warehouse. FR-033 mandates integration test suite.
- **Evidence**: User Story 6 (P1) dedicated to local development environment. FR-045 mandates all test data is mocked.

### Final Gate Assessment

**STATUS**: ✅ **ALL GATES PASS** - No constitutional violations detected

All five core principles are fully addressed in requirements. No complexity tracking needed. Proceed to Phase 0 research.

## Project Structure

### Documentation (this feature)

```
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```
# Airflow DAG-centric project structure
dags/
├── config/                      # JSON DAG configurations
│   ├── schemas/                 # JSON schema definitions
│   └── examples/                # Example configurations for 12+ DAGs
├── factory/                     # Dynamic DAG generation
│   ├── dag_builder.py          # Core DAG factory
│   ├── config_validator.py     # JSON schema validation
│   └── operator_registry.py    # Custom operator registration
└── examples/                    # 12+ example DAGs (Python files)
    ├── beginner/               # Basic patterns
    ├── intermediate/           # Advanced patterns
    └── advanced/               # Expert patterns

src/
├── operators/                   # Custom Airflow operators
│   ├── spark/                  # Spark cluster operators
│   │   ├── standalone_operator.py
│   │   ├── yarn_operator.py
│   │   └── kubernetes_operator.py
│   ├── notifications/          # Notification operators
│   │   ├── email_operator.py
│   │   ├── teams_operator.py
│   │   └── telegram_operator.py
│   └── quality/                # Data quality operators
│       ├── schema_validator.py
│       ├── completeness_checker.py
│       ├── freshness_checker.py
│       ├── uniqueness_checker.py
│       └── null_rate_checker.py
├── hooks/                      # Custom Airflow hooks
│   ├── spark_hook.py
│   └── warehouse_hook.py
├── utils/                      # Utility functions
│   ├── data_generator.py      # Mock data generation
│   ├── logger.py              # Structured logging
│   └── config_loader.py       # Configuration management
└── warehouse/                  # Mock data warehouse setup
    ├── schema.sql             # DDL for warehouse tables
    ├── seed_data.sql          # Initial test data
    └── migrations/            # Database migration scripts

tests/
├── unit/                       # Unit tests for operators, hooks, utils
│   ├── test_operators/
│   ├── test_hooks/
│   └── test_utils/
├── integration/                # Integration tests for DAG execution
│   ├── test_dag_parsing.py
│   ├── test_dag_execution.py
│   └── test_data_quality.py
├── fixtures/                   # Test data fixtures
│   ├── mock_configs/
│   └── mock_datasets/
└── conftest.py                # Pytest configuration

docker/
├── airflow/                   # Airflow container customization
│   └── Dockerfile
├── warehouse/                 # Mock warehouse container
│   └── Dockerfile
└── spark/                     # Spark cluster containers
    ├── Dockerfile.standalone
    ├── Dockerfile.yarn
    └── Dockerfile.k8s

.github/
└── workflows/                 # GitHub Actions CI/CD
    ├── ci.yml                # Lint, test, validate
    ├── cd-staging.yml        # Deploy to staging
    └── cd-production.yml     # Deploy to production

docs/
├── architecture.md           # System architecture
├── dag_configuration.md      # JSON config guide
├── operator_guide.md         # Custom operator usage
└── development.md            # Local setup guide

# Project configuration files
docker-compose.yml            # Local dev environment
requirements.txt              # Python dependencies (pinned versions)
requirements-dev.txt          # Development dependencies
pyproject.toml               # Tool configuration (black, ruff, mypy)
.pre-commit-config.yaml      # Pre-commit hooks
pytest.ini                   # Pytest configuration
.env.example                 # Environment variables template
README.md                    # Project overview and setup
```

**Structure Decision**: Single Python project with DAG-centric organization. The `dags/` directory contains both static example DAGs and the dynamic DAG factory that generates DAGs from JSON configurations. The `src/` directory contains all custom operators, hooks, and utilities organized by functionality. Docker Compose orchestrates local development with Airflow, PostgreSQL warehouse, and Spark clusters. GitHub Actions handles CI/CD. This structure aligns with Airflow best practices while maintaining clean separation of concerns.

## Complexity Tracking

*Fill ONLY if Constitution Check has violations that must be justified*

**No constitutional violations detected.** All requirements align with the five core principles and development workflow standards. No complexity tracking needed.

## Planning Artifacts

### Phase 0: Research (✅ Complete)

**Output**: [research.md](research.md)

**Key Decisions**:
- **Airflow 2.8+**: Latest stable with dynamic DAG generation support
- **Python 3.11**: Modern type hints and performance improvements
- **PostgreSQL 15+**: Dual-purpose (Airflow metadata + mock warehouse)
- **pytest + Great Expectations**: Testing framework with data quality validation
- **ruff + black + mypy**: Code quality toolchain
- **PySpark 3.5+**: Spark integration with simulated clusters
- **GitHub Actions**: CI/CD automation
- **Docker Compose**: Local development orchestration

**Rationale**: All choices prioritize stability, minimalism, educational value, testability, and constitutional compliance.

---

### Phase 1: Design (✅ Complete)

#### Data Model

**Output**: [data-model.md](data-model.md)

**Key Entities**:
- **DAGConfiguration**: JSON schema for dynamic DAG generation
- **TaskDefinition**: Individual task specifications within DAGs
- **DimCustomer, DimProduct, DimDate**: Warehouse dimension tables
- **FactSales**: Warehouse fact table with intentional quality issues
- **QualityCheckResult**: Data quality validation tracking
- **SparkJobSubmission**: Spark job execution metadata
- **NotificationLog**: Notification delivery audit trail

**Design Highlights**:
- Star schema warehouse (dimensions + facts)
- Comprehensive quality metadata
- Operational tracking for observability
- Mock data generation with Faker

---

#### API Contracts

**Output**: [contracts/](contracts/)

**Schemas**:
1. **dag-config-schema.json**: JSON Schema for DAG configurations
   - Validates DAG metadata, tasks, dependencies
   - Enforces naming conventions and structural rules
   - Prevents circular dependencies

2. **operator-parameters-schema.json**: Parameter schemas for all custom operators
   - Spark operators (Standalone, YARN, Kubernetes)
   - Notification operators (Email, Teams, Telegram)
   - Data quality operators (Schema, Completeness, Freshness, Uniqueness, NullRate)

**Validation**: All JSON configs validated against schemas at runtime before DAG instantiation.

---

#### Quickstart Guide

**Output**: [quickstart.md](quickstart.md)

**Sections**:
- Prerequisites and setup (< 5 minutes to running system)
- Environment configuration
- Exploring 12+ example DAGs
- Creating first JSON-configured DAG
- Running tests (unit, integration, DAG validation)
- Monitoring and debugging
- Common operations and troubleshooting

**Target Audience**: Data engineers with basic Airflow knowledge.

---

### Phase 2: Task Generation (⏳ Next Step)

**Command**: `/speckit.tasks`

**Expected Output**: `tasks.md` with implementation tasks organized by user story priority.

**Task Categories**:
- Phase 1: Setup and infrastructure
- Phase 2: Foundational components (DAG factory, base operators)
- Phase 3+: User story implementations (P1 → P2 → P3)
- Final Phase: Polish and cross-cutting concerns

---

## Implementation Readiness

### Ready to Implement ✅

All planning artifacts complete and validated:
- ✅ Technical stack decisions documented
- ✅ Constitution check passed (all 5 principles compliant)
- ✅ Data models defined with sample data strategy
- ✅ API contracts specified with JSON schemas
- ✅ Quickstart guide written for onboarding

### Next Steps

1. Run `/speckit.tasks` to generate dependency-ordered task list
2. Review and approve task breakdown
3. Begin implementation following TDD approach (tests first)
4. Use Docker Compose environment for local integration testing
5. Leverage GitHub Actions CI/CD for automated validation

---

## Key Success Metrics (from Success Criteria)

- **SC-001**: New pipelines via JSON in under 15 minutes
- **SC-003**: Docker Compose startup in under 3 minutes
- **SC-004**: All 12 example DAGs execute in under 5 minutes each
- **SC-005**: Integration tests complete with 100% pass rate in under 10 minutes
- **SC-006**: CI/CD pipeline completes in under 15 minutes
- **SC-007**: 100% data quality anomaly detection in test scenarios
- **SC-010**: 80%+ code coverage for all custom code

---

## Summary

The Apache Airflow ETL Demo Platform is a comprehensive demonstration and learning resource for enterprise-grade data pipeline orchestration. The platform combines:

1. **Dynamic Configuration**: JSON-based DAG generation for rapid pipeline development
2. **Production Patterns**: Failure handling, retries, timeouts, data quality validation
3. **Multi-Cluster Orchestration**: Spark job submission to standalone, YARN, and Kubernetes
4. **Operational Awareness**: Multi-channel notifications and comprehensive monitoring
5. **Educational Value**: 12+ progressively complex example DAGs
6. **Development Excellence**: Full test coverage, CI/CD automation, Docker Compose local environment

All requirements align with constitutional principles (DAG-first, test-driven, quality-assured, maintainable, observable). The platform serves as both a learning resource for students and a reference implementation for production teams.

**Ready for task generation and implementation.**
