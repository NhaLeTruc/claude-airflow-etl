<!--
SYNC IMPACT REPORT
==================
Version Change: Initial → 1.0.0
Rationale: First constitution ratification for Apache Airflow ETL demo project

Modified Principles: N/A (initial creation)
Added Sections:
  - Core Principles (5 principles)
  - Data Quality Standards
  - Development Workflow
  - Governance

Removed Sections: N/A

Templates Status:
  ✅ plan-template.md - Reviewed, Constitution Check section aligns
  ✅ spec-template.md - Reviewed, requirements sections align with principles
  ✅ tasks-template.md - Reviewed, task categorization supports testing and data quality
  ✅ Command files (.claude/commands/*.md) - Reviewed, no agent-specific references

Follow-up TODOs: None
-->

# Apache Airflow ETL Demo Constitution

## Core Principles

### I. DAG-First Development

Every ETL workflow MUST be implemented as an Airflow DAG (Directed Acyclic Graph) with clear, testable components. DAGs MUST be self-contained and independently runnable. Each DAG MUST have:
- Clear, descriptive DAG ID following naming convention: `{domain}_{pipeline_name}_{version}`
- Explicit schedule interval or triggering mechanism
- Documented dependencies and data lineage
- Retry logic and failure handling policies
- Idempotent task design (tasks produce same result when re-run)

**Rationale**: Airflow DAGs are the fundamental unit of orchestration. Clear structure and naming ensures discoverability, maintainability, and reliable execution. Idempotency enables safe retries and recovery from failures.

### II. Test-Driven Data Engineering (NON-NEGOTIABLE)

All DAGs, tasks, and data transformations MUST be thoroughly tested before deployment. Testing MUST follow a multi-layer strategy:
- **Unit tests**: Test individual task logic, operators, and utility functions in isolation
- **Integration tests**: Test DAG structure, task dependencies, and data flow between tasks
- **Data validation tests**: Verify data quality expectations using Great Expectations or similar frameworks
- **DAG validation tests**: Ensure DAGs can be parsed, have no cycles, and meet structural requirements

Tests MUST be written BEFORE implementation. Red-Green-Refactor cycle is strictly enforced. No code merges without passing tests.

**Rationale**: Data engineering failures cascade through pipelines and corrupt downstream systems. Comprehensive testing catches issues before production, ensuring data reliability and reducing debugging time.

### III. Data Quality Assurance (NON-NEGOTIABLE)

Every data pipeline MUST implement explicit data quality checks at critical boundaries. Data quality MUST be:
- **Validated at ingestion**: Check source data completeness, schema conformance, and business rule compliance
- **Monitored during transformation**: Verify row counts, null rates, distribution changes, and referential integrity
- **Certified before output**: Ensure output data meets SLA requirements and downstream consumer expectations
- **Logged and observable**: All quality check results MUST be logged with metrics exposed for monitoring

Failed quality checks MUST halt pipeline execution and trigger alerts. Quality thresholds MUST be explicitly defined and version-controlled.

**Rationale**: Silent data quality failures are catastrophic. Explicit, automated quality gates prevent bad data from propagating, maintain trust in data systems, and enable rapid issue detection.

### IV. Code Quality and Maintainability

All Python code MUST adhere to strict quality standards:
- **Type hints**: All function signatures MUST include type annotations (PEP 484)
- **Linting**: Code MUST pass `ruff` linting with project-defined rules (no warnings)
- **Formatting**: Code MUST be formatted with `black` (line length 100)
- **Complexity**: Functions MUST have cyclomatic complexity ≤ 10 (use `radon` to measure)
- **Documentation**: All DAGs, tasks, and utility functions MUST have docstrings explaining purpose, parameters, returns, and side effects
- **Dependency management**: All dependencies MUST be pinned with exact versions in `requirements.txt`

**Rationale**: Code quality tools catch bugs early, improve readability, and establish consistency. Type hints enable better IDE support and catch type errors at development time. Strict standards make the demo project an exemplar of best practices.

### V. Observability and Debugging

Every DAG and task MUST be designed for easy debugging and operational visibility:
- **Structured logging**: Use Airflow's logging with structured context (DAG ID, task ID, execution date, run ID)
- **Task duration tracking**: All tasks MUST log start time, end time, and duration
- **Data lineage tracking**: Document which datasets each DAG reads from and writes to
- **Failure context**: On task failure, log sufficient context to reproduce the issue (parameters, input data sample, error state)
- **XCom usage**: Minimize XCom usage; when used, document data shape and size constraints
- **Sensor timeout policies**: All sensors MUST have explicit timeout and poke interval configuration

**Rationale**: Debugging distributed data pipelines is challenging. Rich logging and observability enable rapid root cause analysis and reduce mean time to recovery (MTTR).

## Data Quality Standards

### Quality Check Requirements

All data quality checks MUST:
- Be implemented as reusable task templates or custom operators
- Generate standardized quality reports with pass/fail status and metrics
- Store quality check results in a metadata database for historical analysis
- Follow a consistent severity model:
  - **CRITICAL**: Pipeline MUST fail and halt; requires immediate intervention
  - **WARNING**: Pipeline continues but alert is triggered; review required
  - **INFO**: Logged for observability; no action required

### Minimum Quality Checks

Every production DAG MUST implement at minimum:
1. **Schema validation**: Verify expected columns, data types, and constraints
2. **Completeness check**: Verify expected row counts or data volume within tolerance
3. **Freshness check**: Verify source data recency meets SLA requirements
4. **Uniqueness check**: Verify primary key or unique identifier constraints
5. **Null rate check**: Verify critical columns have acceptable null rates

## Development Workflow

### Local Development

All DAGs MUST be testable locally without requiring full Airflow infrastructure:
- Use `pytest` with Airflow's `DAG` test utilities
- Provide mock data fixtures for integration tests
- Document local setup in `README.md` with step-by-step instructions
- Use Docker Compose for local Airflow environment (webserver, scheduler, database)

### Code Review Requirements

All code changes MUST pass review gates:
1. **Automated tests**: All tests pass in CI/CD pipeline
2. **Linting and formatting**: Pre-commit hooks enforce `black`, `ruff`, and `mypy` checks
3. **DAG validation**: DAGs parse successfully and have no structural errors
4. **Peer review**: At least one code review approval required
5. **Documentation**: README and docstrings updated to reflect changes

### Deployment Pipeline

DAGs MUST be deployed through a controlled pipeline:
1. **Feature branch**: Develop on feature branch with prefix `feature/` or `fix/`
2. **CI validation**: Automated tests, linting, and DAG parsing checks on every commit
3. **Staging deployment**: Deploy to staging Airflow environment for end-to-end validation
4. **Production deployment**: After staging validation, deploy to production with rollback plan

## Governance

### Constitution Supremacy

This constitution supersedes all other development practices and decisions. Any deviation from these principles MUST be:
- Explicitly documented with justification in the implementation plan's Complexity Tracking section
- Approved through code review process with constitutional violation acknowledged
- Time-boxed with a plan to return to compliance (if applicable)

### Amendment Process

Constitutional amendments require:
1. **Proposal**: Document proposed change with rationale in GitHub issue or design document
2. **Version bump**: Follow semantic versioning (MAJOR for principle removal/redefinition, MINOR for additions, PATCH for clarifications)
3. **Template sync**: Update all dependent templates (spec, plan, tasks) to reflect new or changed principles
4. **Migration plan**: For breaking changes, provide migration guidance for existing code

### Compliance and Review

All pull requests MUST include a constitutional compliance statement:
- List which principles are exercised (e.g., "Implements Principles II and III")
- Document any violations and justification (referencing plan.md Complexity Tracking section)
- Confirm all quality gates passed

Constitutional compliance reviews occur:
- On every pull request (automated and peer review)
- During retrospectives (monthly review of principle effectiveness)
- When planning new features (Constitution Check section in plan.md)

### Living Documentation

This constitution is a living document. Principle effectiveness is reviewed quarterly based on:
- Developer feedback and pain points
- Production incident retrospectives
- Emerging Airflow best practices and community standards

**Version**: 1.0.0 | **Ratified**: 2025-10-15 | **Last Amended**: 2025-10-15
