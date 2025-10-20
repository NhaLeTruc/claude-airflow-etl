# Tasks: Apache Airflow ETL Demo Platform

**Input**: Design documents from `/specs/001-build-a-full/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are REQUIRED per constitutional Principle II (Test-Driven Data Engineering - NON-NEGOTIABLE). Tests must be written BEFORE implementation following Red-Green-Refactor cycle.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions
- **Single project**: `dags/`, `src/`, `tests/` at repository root
- Paths follow plan.md DAG-centric structure

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [x] T001 Create root project directories (dags/, src/, tests/, docker/, docs/)
- [x] T002 [P] Create Python package structure with __init__.py files in src/ subdirectories
- [x] T003 [P] Create requirements.txt with pinned dependencies (Airflow 2.8.1, PostgreSQL 15, Great Expectations 0.18, PySpark 3.5, pytest 8.0)
- [x] T004 [P] Create requirements-dev.txt with development tools (ruff, black, mypy, pytest-cov, pytest-docker, pre-commit)
- [x] T005 [P] Create pyproject.toml with tool configurations (black line-length=100, ruff rules, mypy strict mode)
- [x] T006 [P] Create pytest.ini with test configuration (markers for unit/integration/slow, coverage settings)
- [x] T007 [P] Create .env.example with environment variables template (Airflow config, PostgreSQL credentials, notification API keys)
- [x] T008 [P] Create README.md with project overview, setup instructions, and quick start guide
- [x] T009 [P] Create .gitignore for Python, Docker, IDE files
- [x] T010 [P] Create pre-commit-config.yaml with hooks (ruff, black, mypy, trailing whitespace)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Database and Warehouse Setup

- [x] T011 Create warehouse schema SQL in src/warehouse/schema.sql (DimCustomer, DimProduct, DimDate, FactSales, QualityCheckResult, SparkJobSubmission, NotificationLog tables)
- [x] T012 Create warehouse seed data SQL in src/warehouse/seed_data.sql (1000 customers, 500 products, 5 years dates, 100k sales with intentional quality issues)
- [x] T013 [P] Create database migration scripts in src/warehouse/migrations/ for schema versioning
- [x] T014 [P] Implement mock data generator utility in src/utils/data_generator.py using Faker library with deterministic seeding

### Docker Environment

- [x] T015 Create Dockerfile for Airflow in docker/airflow/Dockerfile (extends apache/airflow:2.8-python3.11, installs custom dependencies)
- [x] T016 Create Dockerfile for PostgreSQL warehouse in docker/warehouse/Dockerfile (initializes with schema and seed data)
- [x] T017 Create docker-compose.yml with services (airflow-webserver, airflow-scheduler, airflow-init, airflow-warehouse, airflow-postgres)
- [x] T018 Configure volume mounts in docker-compose.yml for dags/, src/, tests/, and database persistence
- [x] T019 Configure health checks for all services in docker-compose.yml (ensure startup order and readiness)

### Base Utilities

- [x] T020 [P] Implement structured logger utility in src/utils/logger.py with Airflow context support (DAG ID, task ID, execution date, run ID)
- [x] T021 [P] Implement configuration loader utility in src/utils/config_loader.py for environment variables and JSON configs
- [x] T022 [P] Create base custom hook in src/hooks/warehouse_hook.py extending PostgresHook for warehouse operations

### Testing Infrastructure

- [x] T023 Create pytest conftest.py with fixtures (mock Airflow context, test database, mock webhook servers)
- [x] T024 [P] Create test fixtures directory structure in tests/fixtures/ (mock_configs/, mock_datasets/)
- [x] T025 [P] Implement pytest markers in pytest.ini (@pytest.mark.unit, @pytest.mark.integration, @pytest.mark.slow)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Dynamic DAG Configuration System (Priority: P1) 🎯 MVP

**Goal**: Enable creation of ETL pipelines via JSON configuration files without writing Python code

**Independent Test**: Create JSON config → Place in dags/config/ → DAG appears in Airflow UI → Execute successfully

### Tests for User Story 1 (TDD: Write tests FIRST, ensure they FAIL before implementation) ⚠️

- [x] T026 [P] [US1] Unit test for JSON schema validation in tests/unit/test_dag_factory/test_config_validator.py (test valid configs pass, invalid fail with specific errors)
- [x] T027 [P] [US1] Unit test for operator registry in tests/unit/test_dag_factory/test_operator_registry.py (test operator registration, lookup, and instantiation)
- [x] T028 [P] [US1] Unit test for DAG builder in tests/unit/test_dag_factory/test_dag_builder.py (test DAG creation from config, task dependency resolution, circular dependency detection)
- [x] T029 [P] [US1] Integration test for DAG parsing in tests/integration/test_dag_parsing.py (test all configs parse without errors, correct DAG count)
- [x] T030 [P] [US1] Integration test for dynamic DAG execution in tests/integration/test_dynamic_dag_execution.py (test JSON-configured DAG runs successfully end-to-end)

### Implementation for User Story 1

- [x] T031 [P] [US1] Copy JSON schemas from contracts/ to dags/config/schemas/ (dag-config-schema.json, operator-parameters-schema.json)
- [x] T032 [US1] Implement JSON config validator in dags/factory/config_validator.py (validate against schema, check circular dependencies, verify operator references)
- [x] T033 [US1] Implement operator registry in dags/factory/operator_registry.py (register built-in and custom operators, provide lookup and instantiation)
- [x] T034 [US1] Implement DAG builder factory in dags/factory/dag_builder.py (read JSON, instantiate DAG, create tasks, set dependencies, handle errors)
- [x] T035 [US1] Create DAG factory main file in dags/factory/__init__.py (discover JSON configs via glob, generate DAGs, expose to Airflow globals())
- [x] T036 [P] [US1] Create example JSON config for simple extract-load in dags/config/examples/simple_extract_load_v1.json
- [x] T037 [P] [US1] Create example JSON config with validation errors for testing in tests/fixtures/mock_configs/invalid_config.json
- [x] T038 [US1] Add logging to DAG factory for config discovery, validation errors, and DAG generation success/failure
- [x] T039 [US1] Document JSON configuration schema and usage in docs/dag_configuration.md

**Checkpoint**: At this point, JSON-configured DAGs should be fully functional and testable independently

---

## Phase 4: User Story 2 - Resilient Pipeline Execution (Priority: P1)

**Goal**: Demonstrate automatic retry, failure handling, timeout management, and graceful degradation

**Independent Test**: Create DAG with intentionally failing tasks → Verify retry behavior → Check error logging → Confirm alert notifications

### Tests for User Story 2 (TDD: Write tests FIRST) ⚠️

- [x] T040 [P] [US2] Unit test for retry policy configuration in tests/unit/test_utils/test_retry_policies.py (test exponential backoff calculation, max retries enforcement)
- [x] T041 [P] [US2] Unit test for timeout handling in tests/unit/test_utils/test_timeout_handler.py (test task termination on timeout, correct status marking)
- [x] T042 [P] [US2] Integration test for retry behavior in tests/integration/test_retry_behavior.py (test task retries 3 times with backoff, then fails permanently)
- [x] T043 [P] [US2] Integration test for failure propagation in tests/integration/test_failure_handling.py (test downstream tasks skip, parallel tasks continue, state recovery after interruption)

### Implementation for User Story 2

- [x] T044 [P] [US2] Implement retry utility functions in src/utils/retry_policies.py (calculate exponential backoff, generate retry delay values)
- [x] T045 [P] [US2] Implement timeout monitoring utility in src/utils/timeout_handler.py (check task execution time, trigger termination)
- [x] T046 [US2] Create example DAG demonstrating retry logic in dags/examples/beginner/demo_scheduled_pipeline_v1.py (configure retries, exponential backoff, timeout)
- [x] T047 [US2] Create example DAG demonstrating failure handling in dags/examples/advanced/demo_failure_recovery_v1.py (intentional task failures, downstream skip, parallel continue, compensation logic)
- [x] T048 [US2] Add structured error logging with full context in all example DAGs (execution date, run ID, task ID, error type, stack trace)
- [x] T049 [P] [US2] Document retry and failure handling patterns in docs/operator_guide.md

**Checkpoint**: Retry, timeout, and failure handling should work correctly across all DAG types

---

## Phase 5: User Story 6 - Local Development Environment (Priority: P1)

**Goal**: Docker Compose environment that starts complete stack locally for integration testing and development

**Independent Test**: Run `docker-compose up` → All services start within 2 minutes → Access Airflow UI → Execute test DAG → Integration tests pass

### Tests for User Story 6 (TDD: Write tests FIRST) ⚠️

- [x] T050 [P] [US6] Integration test for Docker Compose startup in tests/integration/test_docker_environment.py (test all services healthy within timeout)
- [x] T051 [P] [US6] Integration test for DAG hot-reload in tests/integration/test_dag_hotreload.py (test new DAG file appears in UI within 30 seconds)
- [x] T052 [P] [US6] Integration test for environment reset in tests/integration/test_environment_reset.py (test clean state after down -v and restart)

### Implementation for User Story 6

- [x] T053 [US6] Configure Airflow environment variables in docker-compose.yml (executor type, load examples=false, expose config, logging level)
- [x] T054 [US6] Configure warehouse database initialization in docker-compose.yml (run schema.sql and seed_data.sql on first start)
- [x] T055 [US6] Create entrypoint script for Airflow container in docker/airflow/entrypoint.sh (wait for postgres, init DB, create admin user, start services)
- [x] T056 [P] [US6] Configure Airflow connections via environment variables in docker-compose.yml (warehouse postgres connection)
- [x] T057 [P] [US6] Create docker-compose development override file docker-compose.override.yml for faster local iteration (disable unnecessary services)
- [x] T058 [US6] Test Docker Compose startup end-to-end and validate all services reach healthy state
- [x] T059 [P] [US6] Document local development setup in docs/development.md (prerequisites, startup commands, troubleshooting)

**Checkpoint**: Complete local environment working - developers can iterate on features

---

## Phase 6: User Story 3 - Multi-Cluster Spark Job Orchestration (Priority: P2)

**Goal**: Custom operators for submitting Spark jobs to standalone, YARN, and Kubernetes clusters

**Independent Test**: Create DAG with Spark operator for each cluster type → Submit simple Spark job → Verify successful completion → Check log retrieval

### Tests for User Story 3 (TDD: Write tests FIRST) ⚠️

- [x] T060 [P] [US3] Unit test for Spark hook in tests/unit/test_hooks/test_spark_hook.py (test job submission, status polling, log retrieval)
- [x] T061 [P] [US3] Unit test for Spark Standalone operator in tests/unit/test_operators/test_spark_standalone.py (test parameter validation, job submission, error handling)
- [x] T062 [P] [US3] Unit test for Spark YARN operator in tests/unit/test_operators/test_spark_yarn.py (test YARN-specific config, queue selection, resource allocation)
- [x] T063 [P] [US3] Unit test for Spark Kubernetes operator in tests/unit/test_operators/test_spark_kubernetes.py (test K8s namespace, pod creation, cleanup)
- [x] T064 [P] [US3] Integration test for Spark job execution in tests/integration/test_spark_execution.py (test job submission to mocked clusters, status tracking, completion)

### Implementation for User Story 3

- [x] T065 [US3] Implement base Spark hook in src/hooks/spark_hook.py extending BaseHook (connection management, job submission API, status polling, log retrieval)
- [x] T066 [US3] Implement Spark Standalone operator in src/operators/spark/standalone_operator.py (master URL config, application submission, job monitoring)
- [x] T067 [US3] Implement Spark YARN operator in src/operators/spark/yarn_operator.py (queue selection, executor/driver resource config, YARN-specific parameters)
- [x] T068 [US3] Implement Spark Kubernetes operator in src/operators/spark/kubernetes_operator.py (namespace config, pod templates, K8s service account, resource limits)
- [x] T069 [P] [US3] Create sample Spark application in src/spark_apps/word_count.py (simple PySpark job for testing)
- [x] T070 [P] [US3] Create sample Spark application in src/spark_apps/sales_aggregation.py (aggregates mock warehouse data)
- [ ] T071 [P] [US3] Create Spark cluster Docker containers in docker/spark/ (Dockerfile.standalone for local testing)
- [ ] T072 [US3] Register Spark operators in operator registry in dags/factory/operator_registry.py
- [ ] T073 [US3] Create example DAG using Spark Standalone in dags/examples/intermediate/demo_spark_standalone_v1.py
- [ ] T074 [US3] Create example DAG using multi-cluster Spark in dags/examples/advanced/demo_spark_multi_cluster_v1.py (demonstrates all three cluster types)
- [ ] T075 [P] [US3] Document Spark operator usage and configuration in docs/operator_guide.md

**Checkpoint**: Spark job orchestration working for all three cluster types

---

## Phase 7: User Story 4 - Multi-Channel Notification System (Priority: P2)

**Goal**: Custom operators for sending notifications via email, MS Teams, and Telegram

**Independent Test**: Create DAG with notification operators → Trigger on success/failure → Verify message delivery → Check error handling

### Tests for User Story 4 (TDD: Write tests FIRST) ⚠️

- [ ] T076 [P] [US4] Unit test for base notification operator in tests/unit/test_operators/test_base_notification.py (test retry logic, error handling, fallback mechanisms)
- [ ] T077 [P] [US4] Unit test for email operator in tests/unit/test_operators/test_email_notification.py (test SMTP connection, template rendering, recipient validation)
- [ ] T078 [P] [US4] Unit test for Teams operator in tests/unit/test_operators/test_teams_notification.py (test webhook POST, message card formatting, error handling)
- [ ] T079 [P] [US4] Unit test for Telegram operator in tests/unit/test_operators/test_telegram_notification.py (test bot API, message formatting, chat ID validation)
- [ ] T080 [P] [US4] Integration test for notification delivery in tests/integration/test_notification_delivery.py (test messages sent to mock endpoints, correct content)

### Implementation for User Story 4

- [ ] T081 [US4] Implement base notification operator in src/operators/notifications/base_notification.py (retry logic, timeout, template rendering with Jinja2, error logging)
- [ ] T082 [US4] Implement email notification operator in src/operators/notifications/email_operator.py (SMTP connection, HTML/plain text support, attachment handling)
- [ ] T083 [US4] Implement MS Teams notification operator in src/operators/notifications/teams_operator.py (webhook POST, message card builder, theme color support, facts/actions)
- [ ] T084 [US4] Implement Telegram notification operator in src/operators/notifications/telegram_operator.py (bot API integration, markdown/HTML parsing, silent notification option)
- [ ] T085 [P] [US4] Create notification message templates in src/utils/notification_templates.py (success, failure, data quality alert templates with Jinja2)
- [ ] T086 [US4] Register notification operators in operator registry in dags/factory/operator_registry.py
- [ ] T087 [US4] Create example DAG demonstrating notifications in dags/examples/beginner/demo_notification_basics_v1.py (email on success, Teams on failure)
- [ ] T088 [P] [US4] Document notification operator configuration and templating in docs/operator_guide.md

**Checkpoint**: Multi-channel notifications working with proper error handling

---

## Phase 8: Foundational Data Quality Operators

**Purpose**: Reusable quality operators needed by multiple user stories (especially US5)

### Tests for Data Quality Operators (TDD: Write tests FIRST) ⚠️

- [ ] T089 [P] Unit test for base quality operator in tests/unit/test_operators/test_base_quality.py (test severity levels, threshold validation, result logging)
- [ ] T090 [P] Unit test for schema validator in tests/unit/test_operators/test_schema_validator.py (test column validation, type checking, error reporting)
- [ ] T091 [P] Unit test for completeness checker in tests/unit/test_operators/test_completeness_checker.py (test row count validation, tolerance calculation)
- [ ] T092 [P] Unit test for freshness checker in tests/unit/test_operators/test_freshness_checker.py (test timestamp comparison, max age validation)
- [ ] T093 [P] Unit test for uniqueness checker in tests/unit/test_operators/test_uniqueness_checker.py (test duplicate detection, composite key support)
- [ ] T094 [P] Unit test for null rate checker in tests/unit/test_operators/test_null_rate_checker.py (test null percentage calculation, threshold comparison)
- [ ] T095 [P] Integration test for quality check execution in tests/integration/test_data_quality_execution.py (test quality checks run against warehouse, results stored correctly)

### Implementation of Data Quality Operators

- [ ] T096 [US5] Implement base quality operator in src/operators/quality/base_quality_operator.py (severity enum, result storage, threshold validation, structured logging)
- [ ] T097 [P] [US5] Implement schema validation operator in src/operators/quality/schema_validator.py (column list check, data type validation, missing/extra column detection)
- [ ] T098 [P] [US5] Implement completeness check operator in src/operators/quality/completeness_checker.py (row count query, min/max/expected validation, tolerance percentage)
- [ ] T099 [P] [US5] Implement freshness check operator in src/operators/quality/freshness_checker.py (max timestamp query, age calculation, SLA comparison)
- [ ] T100 [P] [US5] Implement uniqueness check operator in src/operators/quality/uniqueness_checker.py (GROUP BY with COUNT, duplicate row identification, composite key support)
- [ ] T101 [P] [US5] Implement null rate check operator in src/operators/quality/null_rate_checker.py (NULL count per column, percentage calculation, threshold validation)
- [ ] T102 [US5] Register all quality operators in operator registry in dags/factory/operator_registry.py
- [ ] T103 [P] [US5] Create Great Expectations integration utility in src/utils/great_expectations_helper.py (expectation suite loader, validation result parser)

**Checkpoint**: All data quality operators implemented and tested

---

## Phase 9: User Story 5 - Comprehensive DAG Example Gallery (Priority: P3)

**Goal**: 12+ example DAGs demonstrating progressively complex ETL patterns

**Independent Test**: Execute each example DAG independently → Verify successful completion → Confirm pattern demonstration

### Tests for Example DAGs (TDD: Write tests FIRST) ⚠️

- [ ] T104 [P] [US5] Integration test for all example DAGs in tests/integration/test_example_dags.py (test each DAG executes successfully in under 5 minutes)
- [ ] T105 [P] [US5] Integration test for idempotency in tests/integration/test_dag_idempotency.py (test incremental DAG runs multiple times without duplicates)
- [ ] T106 [P] [US5] Integration test for data quality detection in tests/integration/test_quality_detection.py (test quality DAG detects all injected anomalies)

### Beginner Example DAGs

- [ ] T107 [P] [US5] Create simple extract-load DAG in dags/examples/beginner/demo_simple_extract_load_v1.py (PostgresOperator extract → load to staging)
- [ ] T108 [P] [US5] Create scheduled pipeline DAG in dags/examples/beginner/demo_scheduled_pipeline_v1.py (daily schedule, retry config, timeout)
- [ ] T109 [P] [US5] Create data quality basics DAG in dags/examples/beginner/demo_data_quality_basics_v1.py (schema validation, completeness check)
- [ ] T110 [P] [US5] Create notification basics DAG in dags/examples/beginner/demo_notification_basics_v1.py (email on failure, Teams on success)

### Intermediate Example DAGs

- [ ] T111 [P] [US5] Create incremental load DAG in dags/examples/intermediate/demo_incremental_load_v1.py (watermark tracking, process only new records, idempotent)
- [ ] T112 [P] [US5] Create SCD Type 2 DAG in dags/examples/intermediate/demo_scd_type2_v1.py (dimension history tracking, effective dates, current flag)
- [ ] T113 [P] [US5] Create parallel processing DAG in dags/examples/intermediate/demo_parallel_processing_v1.py (fan-out pattern, multiple branches, join)
- [ ] T114 [P] [US5] Create Spark standalone DAG in dags/examples/intermediate/demo_spark_standalone_v1.py (submit word count job, monitor status)
- [ ] T115 [P] [US5] Create cross-DAG dependency in dags/examples/intermediate/demo_cross_dag_dependency_v1.py (ExternalTaskSensor, TriggerDagRunOperator)

### Advanced Example DAGs

- [ ] T116 [P] [US5] Create multi-cluster Spark DAG in dags/examples/advanced/demo_spark_multi_cluster_v1.py (jobs to standalone, YARN, K8s)
- [ ] T117 [P] [US5] Create comprehensive quality DAG in dags/examples/advanced/demo_comprehensive_quality_v1.py (all 5 quality checks, severity levels, alerting)
- [ ] T118 [P] [US5] Create event-driven pipeline DAG in dags/examples/advanced/demo_event_driven_pipeline_v1.py (FileSensor, trigger on file arrival)
- [ ] T119 [P] [US5] Create failure recovery DAG in dags/examples/advanced/demo_failure_recovery_v1.py (compensation tasks, cleanup on failure, state recovery)

### Documentation for Example DAGs

- [ ] T120 [US5] Add inline documentation to all example DAGs (docstrings explaining pattern, use case, Airflow features)
- [ ] T121 [P] [US5] Create example DAG catalog in docs/dag_examples_catalog.md (index of all examples with descriptions and learning objectives)

**Checkpoint**: All 13 example DAGs complete and independently executable

---

## Phase 10: User Story 7 - CI/CD Pipeline (Priority: P2)

**Goal**: Automated GitHub Actions pipeline for linting, testing, validation, and deployment

**Independent Test**: Push code change → CI runs automatically → View pipeline results → Deploy to staging → Approve prod deployment

### Tests for CI/CD (Validation tests) ⚠️

- [ ] T122 [P] [US7] Create CI workflow validation test in tests/integration/test_ci_workflow.py (test workflow YAML syntax, job dependencies)
- [ ] T123 [P] [US7] Create deployment smoke tests in tests/integration/test_deployment_smoke.py (test basic DAG functionality post-deployment)

### Implementation for User Story 7

- [ ] T124 [US7] Create CI workflow in .github/workflows/ci.yml (runs on push/PR: checkout, setup Python, install deps, run ruff/black/mypy, run pytest with coverage)
- [ ] T125 [US7] Add DAG parsing validation job to CI workflow (load all DAGs with DagBag, check for import errors)
- [ ] T126 [US7] Add coverage reporting to CI workflow (upload coverage to codecov or generate HTML report)
- [ ] T127 [US7] Create staging deployment workflow in .github/workflows/cd-staging.yml (triggers on merge to main: deploy DAGs to staging, run smoke tests)
- [ ] T128 [US7] Create production deployment workflow in .github/workflows/cd-production.yml (manual approval gate, deploy to prod with rollback capability)
- [ ] T129 [P] [US7] Configure GitHub branch protection rules in docs/ci_cd_setup.md (require CI pass, code review, no force push to main)
- [ ] T130 [P] [US7] Document CI/CD pipeline architecture and deployment process in docs/ci_cd_pipeline.md

**Checkpoint**: Full CI/CD pipeline operational with automated validation

---

## Phase 11: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T131 [P] Documentation updates in README.md (add badges for build status, coverage, Python version)
- [ ] T132 [P] Create architecture diagram in docs/architecture.md (system components, data flow, integration points)
- [ ] T133 [P] Create comprehensive operator guide in docs/operator_guide.md (all custom operators, parameters, examples)
- [ ] T134 [P] Review and update quickstart.md based on actual implementation
- [ ] T135 Code cleanup and refactoring (remove dead code, improve naming, extract common utilities)
- [ ] T136 [P] Performance optimization (DAG parsing speed, operator execution efficiency)
- [ ] T137 [P] Security hardening (review credentials handling, validate input sanitization, check for SQL injection risks)
- [ ] T138 [P] Additional unit tests to reach 80%+ coverage target (identify gaps with pytest-cov --cov-report=term-missing)
- [ ] T139 Run full integration test suite and validate 100% pass rate
- [ ] T140 Validate quickstart.md instructions on fresh machine (test setup from scratch)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational phase completion
- **User Story 2 (Phase 4)**: Depends on Foundational phase completion
- **User Story 6 (Phase 5)**: Depends on Foundational phase completion
- **User Story 3 (Phase 6)**: Depends on US1 (needs DAG factory and registry)
- **User Story 4 (Phase 7)**: Depends on US1 (needs operator registry)
- **Data Quality Operators (Phase 8)**: Depends on US1 (needs operator registry)
- **User Story 5 (Phase 9)**: Depends on US1, US2, US3, US4, Phase 8 (needs all operators)
- **User Story 7 (Phase 10)**: Depends on US1, US2, US6 (needs testable code and Docker environment)
- **Polish (Phase 11)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Foundational → US1 (no dependencies on other stories)
- **User Story 2 (P1)**: Foundational → US2 (no dependencies on other stories)
- **User Story 6 (P1)**: Foundational → US6 (no dependencies on other stories)
- **User Story 3 (P2)**: Foundational → US1 → US3 (needs operator registry)
- **User Story 4 (P2)**: Foundational → US1 → US4 (needs operator registry)
- **User Story 5 (P3)**: Foundational → US1, US2, US3, US4, Quality Ops → US5 (needs all features)
- **User Story 7 (P2)**: Foundational → US1, US2, US6 → US7 (needs code to test and deploy)

### Within Each User Story

- **Tests MUST be written and FAIL before implementation** (Red-Green-Refactor cycle)
- Models/entities before services
- Services before operators
- Operators before example DAGs
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- **Setup (Phase 1)**: All tasks marked [P] can run in parallel (T002-T010)
- **Foundational (Phase 2)**: Database setup, Docker setup, utilities, tests can run in parallel
- **Within User Stories**: All tasks marked [P] can run in parallel (typically tests, independent modules)
- **Across User Stories**: US1, US2, US6 can start in parallel after Foundational; US3, US4 can start in parallel after US1

---

## Parallel Example: User Story 3 (Spark Orchestration)

```bash
# Launch all unit tests for US3 together:
Task T060: "Unit test for Spark hook in tests/unit/test_hooks/test_spark_hook.py"
Task T061: "Unit test for Spark Standalone operator in tests/unit/test_operators/test_spark_standalone.py"
Task T062: "Unit test for Spark YARN operator in tests/unit/test_operators/test_spark_yarn.py"
Task T063: "Unit test for Spark Kubernetes operator in tests/unit/test_operators/test_spark_kubernetes.py"

# After tests written and failing, launch parallel implementations:
Task T066: "Implement Spark Standalone operator in src/operators/spark/standalone_operator.py"
Task T067: "Implement Spark YARN operator in src/operators/spark/yarn_operator.py"
Task T068: "Implement Spark Kubernetes operator in src/operators/spark/kubernetes_operator.py"
Task T069: "Create sample Spark application in src/spark_apps/word_count.py"
Task T070: "Create sample Spark application in src/spark_apps/sales_aggregation.py"
```

---

## Implementation Strategy

### MVP First (Core P1 User Stories Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1 (Dynamic DAG Configuration) → **TEST independently**
4. Complete Phase 4: User Story 2 (Resilient Execution) → **TEST independently**
5. Complete Phase 5: User Story 6 (Local Dev Environment) → **TEST independently**
6. **STOP and VALIDATE**: All three P1 stories working independently
7. Deploy/demo MVP

**MVP Delivers**:
- JSON-based DAG generation (US1)
- Production-grade failure handling (US2)
- Full local development environment (US6)
- Comprehensive test coverage
- Docker Compose for easy setup

### Incremental Delivery (Add P2 Features)

After MVP validation:
8. Complete Phase 6: User Story 3 (Spark Orchestration) → **TEST independently**
9. Complete Phase 7: User Story 4 (Multi-Channel Notifications) → **TEST independently**
10. Complete Phase 8: Data Quality Operators (Foundation for US5)
11. Complete Phase 10: User Story 7 (CI/CD Pipeline) → **TEST independently**
12. Deploy/demo enhanced platform

**Enhanced Platform Delivers**:
- Everything in MVP plus:
- Multi-cluster Spark job orchestration (US3)
- Email, Teams, Telegram notifications (US4)
- Data quality operator library
- Automated CI/CD with GitHub Actions (US7)

### Full Platform (Add P3 Breadth)

After enhanced platform validation:
13. Complete Phase 9: User Story 5 (12+ Example DAGs) → **TEST each independently**
14. Complete Phase 11: Polish & Cross-Cutting Concerns
15. Final validation of all success criteria
16. Production-ready release

**Full Platform Delivers**:
- Everything in Enhanced Platform plus:
- 12+ example DAGs covering beginner to advanced patterns (US5)
- Comprehensive documentation and learning resources
- Production-grade polish and optimization
- Ready for forking and customization

### Parallel Team Strategy

With multiple developers:

1. **Team completes Setup + Foundational together** (critical path)
2. **Once Foundational is done, split team**:
   - Developer A: User Story 1 (Dynamic DAG Configuration)
   - Developer B: User Story 2 (Resilient Execution)
   - Developer C: User Story 6 (Local Dev Environment)
3. **After P1 stories complete, reassign**:
   - Developer A: User Story 3 (Spark Orchestration)
   - Developer B: User Story 4 (Notifications)
   - Developer C: Data Quality Operators + User Story 7 (CI/CD)
4. **Final phase all team together**:
   - Split 12 example DAGs across team (4 each)
   - Pair on polish and cross-cutting concerns

---

## Notes

- **[P] tasks** = different files, no dependencies on incomplete tasks
- **[Story] label** maps task to specific user story for traceability
- **TDD mandate**: Tests MUST be written first and MUST fail before implementation (constitutional requirement)
- Each user story should be independently completable and testable
- Verify tests fail before implementing (Red-Green-Refactor)
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- 80%+ code coverage is mandatory (constitutional requirement)
- All DAGs must follow naming convention: `{domain}_{pipeline_name}_{version}`
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

---

## Success Criteria Validation Checklist

Upon completion, verify these success criteria from spec.md:

- [ ] **SC-001**: New pipelines created via JSON in under 15 minutes
- [ ] **SC-002**: Task retry succeeds within 3 attempts for 95% of transient errors
- [ ] **SC-003**: Docker Compose starts in under 3 minutes on fresh machine
- [ ] **SC-004**: All 12 example DAGs execute in under 5 minutes each
- [ ] **SC-005**: Integration test suite completes with 100% pass rate in under 10 minutes
- [ ] **SC-006**: CI/CD pipeline completes validation in under 15 minutes
- [ ] **SC-007**: Data quality checks detect 100% of intentionally injected anomalies
- [ ] **SC-008**: Notifications delivered within 60 seconds in 99% of cases
- [ ] **SC-009**: Spark operators submit/monitor jobs to all three cluster types successfully
- [ ] **SC-010**: Code coverage meets or exceeds 80% for all custom code
- [ ] **SC-011**: Developer can set up and create new DAG in under 2 hours following docs
- [ ] **SC-012**: Platform serves as forkable reference implementation within one sprint

---

**Total Tasks**: 140
**MVP Tasks (P1 only)**: T001-T059 (59 tasks)
**Enhanced Platform Tasks (P1 + P2)**: T001-T103, T122-T130 (112 tasks)
**Full Platform Tasks (All)**: T001-T140 (140 tasks)

**Estimated Parallel Opportunities**: 60+ tasks can run in parallel across phases
**Independent Test Criteria**: Each user story has clear acceptance tests
**Suggested MVP Scope**: User Stories 1, 2, 6 (Dynamic DAGs, Resilience, Local Environment)
