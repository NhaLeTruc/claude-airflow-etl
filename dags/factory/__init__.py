"""
DAG factory for dynamic DAG generation from JSON configurations.

This module discovers JSON configurations in dags/config/examples/
and generates Airflow DAGs dynamically. The generated DAGs are exposed
to Airflow via the module's globals().
"""

import glob
from pathlib import Path

from airflow import DAG

from dags.factory.dag_builder import DAGBuilder, DAGBuildError
from src.utils.logger import get_logger

logger = get_logger(__name__)


def discover_and_generate_dags() -> dict[str, DAG]:
    """
    Discover JSON configurations and generate DAGs.

    Searches dags/config/examples/ for *.json files and creates DAGs from them.
    Errors are logged but don't prevent other DAGs from being created.

    Returns:
        Dict of dag_id -> DAG instance for successfully created DAGs
    """
    logger.info("Starting DAG discovery and generation")

    # Find config directory
    factory_dir = Path(__file__).parent
    dags_dir = factory_dir.parent
    config_dir = dags_dir / "config" / "examples"

    if not config_dir.exists():
        logger.warning(
            "Config directory not found, no DAGs will be generated",
            config_dir=str(config_dir),
        )
        return {}

    # Discover JSON config files
    config_pattern = str(config_dir / "*.json")
    config_files = glob.glob(config_pattern)

    if not config_files:
        logger.info("No JSON configuration files found", config_dir=str(config_dir))
        return {}

    logger.info(
        "Found configuration files",
        count=len(config_files),
        files=[Path(f).name for f in config_files],
    )

    # Generate DAGs from configs
    builder = DAGBuilder()
    generated_dags: dict[str, DAG] = {}

    for config_file in config_files:
        config_path = Path(config_file)
        try:
            logger.debug("Processing configuration file", config_file=config_path.name)

            dag = builder.build_dag_from_file(str(config_path))
            generated_dags[dag.dag_id] = dag

            logger.info(
                "DAG generated successfully",
                dag_id=dag.dag_id,
                config_file=config_path.name,
                task_count=len(dag.tasks),
            )

        except DAGBuildError as e:
            logger.error(
                "Failed to build DAG from config",
                config_file=config_path.name,
                error=str(e),
            )
        except Exception as e:
            logger.exception(
                "Unexpected error building DAG",
                config_file=config_path.name,
                error=str(e),
            )

    logger.info(
        "DAG generation complete",
        total_configs=len(config_files),
        successful=len(generated_dags),
        failed=len(config_files) - len(generated_dags),
    )

    return generated_dags


# Generate DAGs and expose them to Airflow
try:
    logger.info("Initializing DAG factory")
    dags = discover_and_generate_dags()

    # Add DAGs to module globals so Airflow can discover them
    for dag_id, dag in dags.items():
        globals()[dag_id] = dag
        logger.debug("DAG exposed to Airflow globals", dag_id=dag_id)

    logger.info("DAG factory initialization complete", dag_count=len(dags))

except Exception as e:
    logger.exception("Fatal error during DAG factory initialization", error=str(e))
    # Re-raise to prevent Airflow from running with partial DAG set
    raise


# Export public API
__all__ = [
    "config_validator",
    "operator_registry",
    "dag_builder",
    "discover_and_generate_dags",
]
