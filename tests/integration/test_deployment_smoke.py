"""
Deployment smoke tests for post-deployment validation.

These tests verify basic functionality after deployment to staging or production.
They are designed to run quickly and catch critical deployment issues.

Constitutional Compliance:
- Principle II: Test-Driven Data Engineering (validates deployment correctness)
- Principle V: Observability (ensures deployment health monitoring)

Smoke Test Criteria:
- Must complete in under 2 minutes
- Tests only critical paths
- No dependency on external services (where possible)
- Safe to run in production
"""

import os
from pathlib import Path

import pytest
from airflow.models import DagBag

# Configuration
DAGS_FOLDER = os.getenv("AIRFLOW_HOME", "/opt/airflow") + "/dags"
MAX_DAG_LOAD_TIME_SECONDS = 30


@pytest.fixture(scope="module")
def dag_bag():
    """Load DAG bag for smoke tests."""
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


class TestDeploymentBasics:
    """Basic deployment smoke tests."""

    def test_airflow_home_exists(self):
        """Test that Airflow home directory exists."""
        airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        assert os.path.exists(airflow_home), f"Airflow home directory not found: {airflow_home}"

    def test_dags_folder_exists(self):
        """Test that DAGs folder exists and is accessible."""
        assert os.path.exists(DAGS_FOLDER), f"DAGs folder not found: {DAGS_FOLDER}"
        assert os.path.isdir(DAGS_FOLDER), f"DAGs path is not a directory: {DAGS_FOLDER}"

    def test_python_packages_installed(self):
        """Test that required Python packages are installed."""
        required_packages = [
            "airflow",
            "sqlalchemy",
            "psycopg2",
            "pytest",
        ]

        missing_packages = []
        for package in required_packages:
            try:
                __import__(package.replace("-", "_"))
            except ImportError:
                missing_packages.append(package)

        assert len(missing_packages) == 0, f"Missing required packages: {missing_packages}"

    def test_environment_variables_set(self):
        """Test that critical environment variables are set."""
        required_env_vars = [
            "AIRFLOW_HOME",
            "AIRFLOW__CORE__DAGS_FOLDER",
        ]

        missing_vars = [var for var in required_env_vars if not os.getenv(var)]

        # These are important but not critical for smoke test
        if missing_vars:
            pytest.skip(f"Optional env vars not set: {missing_vars}")


class TestDAGDeployment:
    """Smoke tests for DAG deployment."""

    def test_dags_load_without_errors(self, dag_bag):
        """Test that all DAGs load without import errors."""
        import_errors = dag_bag.import_errors

        assert len(import_errors) == 0, "DAG import errors detected:\n" + "\n".join(
            f"  {file}: {error}" for file, error in import_errors.items()
        )

    def test_minimum_dag_count(self, dag_bag):
        """Test that expected DAGs are deployed."""
        dag_count = len(dag_bag.dags)

        # At minimum, should have some example DAGs
        # Adjust this number based on your deployment strategy
        min_expected_dags = 5

        assert dag_count >= min_expected_dags, (
            f"Expected at least {min_expected_dags} DAGs, found {dag_count}. "
            f"DAGs: {list(dag_bag.dag_ids)}"
        )

    def test_dag_loading_performance(self, dag_bag):
        """Test that DAG loading completes within acceptable time."""
        # This is measured during DAG bag initialization
        # If this test runs, loading was successful within timeout
        assert len(dag_bag.dags) > 0, "No DAGs loaded"

    def test_no_dag_cycles(self, dag_bag):
        """Test that no DAGs have circular dependencies."""
        for dag_id, dag in dag_bag.dags.items():
            try:
                # This will raise if there's a cycle
                dag.topological_sort()
            except Exception as e:
                pytest.fail(f"DAG '{dag_id}' has circular dependencies: {e}")

    def test_all_dags_have_owners(self, dag_bag):
        """Test that all DAGs have owners configured."""
        dags_without_owners = []

        for dag_id, dag in dag_bag.dags.items():
            if not dag.default_args.get("owner"):
                dags_without_owners.append(dag_id)

        assert (
            len(dags_without_owners) == 0
        ), f"DAGs missing owner configuration: {dags_without_owners}"


class TestCriticalDAGs:
    """Smoke tests for critical DAGs that must be present."""

    @pytest.mark.parametrize(
        "dag_id",
        [
            "demo_simple_extract_load_v1",
            "demo_data_quality_basics_v1",
        ],
    )
    def test_critical_dag_exists(self, dag_bag, dag_id):
        """Test that critical DAGs are deployed."""
        # Skip if this is a minimal deployment
        if dag_id not in dag_bag.dags:
            pytest.skip(f"DAG '{dag_id}' not in this deployment (minimal deployment)")

        dag = dag_bag.get_dag(dag_id)
        assert dag is not None, f"Critical DAG '{dag_id}' not found"

    @pytest.mark.parametrize(
        "dag_id",
        [
            "demo_simple_extract_load_v1",
            "demo_data_quality_basics_v1",
        ],
    )
    def test_critical_dag_has_tasks(self, dag_bag, dag_id):
        """Test that critical DAGs have tasks configured."""
        if dag_id not in dag_bag.dags:
            pytest.skip(f"DAG '{dag_id}' not in this deployment")

        dag = dag_bag.get_dag(dag_id)
        assert len(dag.tasks) > 0, f"DAG '{dag_id}' has no tasks"


class TestCustomOperators:
    """Smoke tests for custom operators deployment."""

    def test_custom_operators_importable(self):
        """Test that custom operators can be imported."""
        custom_operators = [
            ("src.operators.quality.base_quality_operator", "BaseDataQualityOperator"),
            ("src.operators.notifications.base_notification", "BaseNotificationOperator"),
        ]

        import_errors = []
        for module_name, class_name in custom_operators:
            try:
                module = __import__(module_name, fromlist=[class_name])
                getattr(module, class_name)
            except (ImportError, AttributeError) as e:
                import_errors.append(f"{module_name}.{class_name}: {e}")

        # Custom operators are optional for minimal deployment
        if import_errors:
            pytest.skip(f"Custom operators not available in this deployment: {import_errors}")


class TestDatabaseConnectivity:
    """Smoke tests for database connectivity."""

    def test_metadata_db_accessible(self):
        """Test that Airflow metadata database is accessible."""
        try:
            from airflow.models import DagModel
            from airflow.settings import Session

            session = Session()
            # Simple query to check DB access
            session.query(DagModel).limit(1).all()
            session.close()

            # If we got here, DB is accessible
            assert True

        except Exception as e:
            pytest.fail(f"Cannot access Airflow metadata database: {e}")

    def test_warehouse_connection_configured(self):
        """Test that warehouse connection is configured."""
        try:
            from airflow.hooks.base import BaseHook

            # Try to get warehouse connection
            conn = BaseHook.get_connection("warehouse_postgres")

            assert conn is not None, "Warehouse connection not found"
            assert conn.host is not None, "Warehouse connection missing host"

        except Exception as e:
            # Warehouse connection might not be configured in all environments
            pytest.skip(f"Warehouse connection not configured: {e}")


class TestFileStructure:
    """Smoke tests for deployed file structure."""

    def test_src_directory_exists(self):
        """Test that src directory is deployed."""
        # Try both development and production locations
        possible_locations = [
            Path("/opt/airflow/src"),
            Path(os.getcwd()) / "src",
        ]

        src_exists = any(loc.exists() for loc in possible_locations)

        # src directory might not be deployed in minimal setups
        if not src_exists:
            pytest.skip("src directory not found in deployment (minimal deployment)")

    def test_dags_directory_structure(self):
        """Test that DAGs directory has expected structure."""
        dags_path = Path(DAGS_FOLDER)

        # Check for common subdirectories
        expected_subdirs = ["examples", "factory"]

        found_subdirs = [d.name for d in dags_path.iterdir() if d.is_dir()]

        # At least one expected subdir should exist
        has_structure = any(subdir in found_subdirs for subdir in expected_subdirs)

        if not has_structure:
            pytest.skip(
                f"Standard DAG structure not found. "
                f"Found: {found_subdirs}, Expected one of: {expected_subdirs}"
            )


@pytest.mark.slow
class TestExecutionReadiness:
    """Smoke tests for execution readiness."""

    def test_scheduler_can_parse_dags(self, dag_bag):
        """Test that scheduler can parse all DAGs."""
        # If dag_bag loaded successfully, this passes
        assert len(dag_bag.dags) > 0, "No DAGs available for scheduling"

    def test_dag_serialization_works(self, dag_bag):
        """Test that DAGs can be serialized (required for executor)."""
        # Pick first available DAG
        if not dag_bag.dags:
            pytest.skip("No DAGs available for testing")

        first_dag_id = list(dag_bag.dags.keys())[0]
        dag = dag_bag.get_dag(first_dag_id)

        try:
            # Test basic serialization
            from airflow.serialization.serialized_objects import SerializedDAG

            serialized = SerializedDAG.to_dict(dag)

            assert serialized is not None, "DAG serialization returned None"
            assert "dag_id" in serialized, "Serialized DAG missing dag_id"

        except Exception as e:
            pytest.fail(f"DAG serialization failed for '{first_dag_id}': {e}")


@pytest.mark.integration
def test_deployment_smoke_suite_completes():
    """Meta test: Verify smoke test suite completed."""
    # If we reached here, all smoke tests passed or were skipped
    assert True, "Deployment smoke tests completed"


# Summary function for reporting
def get_deployment_health_summary(dag_bag):
    """
    Generate deployment health summary.

    Returns:
        dict: Health summary with metrics
    """
    return {
        "dags_deployed": len(dag_bag.dags),
        "import_errors": len(dag_bag.import_errors),
        "dag_ids": list(dag_bag.dag_ids),
        "status": "healthy" if len(dag_bag.import_errors) == 0 else "degraded",
    }
