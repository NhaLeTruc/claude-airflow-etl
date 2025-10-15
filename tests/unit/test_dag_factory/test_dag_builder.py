"""
Unit tests for DAG builder factory.

Tests DAG creation from config, task dependency resolution,
circular dependency detection, and error handling.
"""

from datetime import datetime

import pytest
from airflow import DAG
from airflow.operators.bash import BashOperator


class TestDAGBuilder:
    """Test suite for DAGBuilder class."""

    def test_build_dag_from_valid_config(self, sample_dag_config):
        """Test building DAG from valid configuration."""
        from dags.factory.dag_builder import DAGBuilder

        builder = DAGBuilder()
        dag = builder.build_dag(sample_dag_config)

        assert isinstance(dag, DAG)
        assert dag.dag_id == sample_dag_config["dag_id"]
        assert dag.description == sample_dag_config["description"]

    def test_dag_has_correct_schedule_interval(self, sample_dag_config):
        """Test that built DAG has correct schedule interval."""
        from dags.factory.dag_builder import DAGBuilder

        builder = DAGBuilder()
        dag = builder.build_dag(sample_dag_config)

        assert dag.schedule_interval == sample_dag_config["schedule_interval"]

    def test_dag_has_correct_start_date(self, sample_dag_config):
        """Test that built DAG has correct start date."""
        from dags.factory.dag_builder import DAGBuilder

        builder = DAGBuilder()
        dag = builder.build_dag(sample_dag_config)

        expected_date = datetime.strptime(sample_dag_config["start_date"], "%Y-%m-%d")
        assert dag.start_date.date() == expected_date.date()

    def test_dag_has_all_tasks_from_config(self, sample_dag_config):
        """Test that all tasks from config are created in DAG."""
        from dags.factory.dag_builder import DAGBuilder

        builder = DAGBuilder()
        dag = builder.build_dag(sample_dag_config)

        task_ids_in_config = [task["task_id"] for task in sample_dag_config["tasks"]]
        task_ids_in_dag = [task.task_id for task in dag.tasks]

        assert len(task_ids_in_dag) == len(task_ids_in_config)
        for task_id in task_ids_in_config:
            assert task_id in task_ids_in_dag

    def test_task_dependencies_set_correctly(self):
        """Test that task dependencies are set correctly."""
        from dags.factory.dag_builder import DAGBuilder

        config = {
            "dag_id": "dependency_test_dag",
            "description": "Test DAG for dependencies",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tasks": [
                {
                    "task_id": "extract",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo 'Extracting'"},
                },
                {
                    "task_id": "transform",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo 'Transforming'"},
                    "dependencies": ["extract"],
                },
                {
                    "task_id": "load",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo 'Loading'"},
                    "dependencies": ["transform"],
                },
            ],
        }

        builder = DAGBuilder()
        dag = builder.build_dag(config)

        extract_task = dag.task_dict["extract"]
        transform_task = dag.task_dict["transform"]
        load_task = dag.task_dict["load"]

        # Check upstream dependencies
        assert extract_task in transform_task.upstream_list
        assert transform_task in load_task.upstream_list

        # Check downstream dependencies
        assert transform_task in extract_task.downstream_list
        assert load_task in transform_task.downstream_list

    def test_multiple_dependencies_handled_correctly(self):
        """Test that tasks with multiple dependencies work correctly."""
        from dags.factory.dag_builder import DAGBuilder

        config = {
            "dag_id": "multi_dependency_dag",
            "description": "Test DAG for multiple dependencies",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tasks": [
                {
                    "task_id": "extract_a",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo 'Extract A'"},
                },
                {
                    "task_id": "extract_b",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo 'Extract B'"},
                },
                {
                    "task_id": "merge",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo 'Merging'"},
                    "dependencies": ["extract_a", "extract_b"],
                },
            ],
        }

        builder = DAGBuilder()
        dag = builder.build_dag(config)

        merge_task = dag.task_dict["merge"]
        extract_a_task = dag.task_dict["extract_a"]
        extract_b_task = dag.task_dict["extract_b"]

        assert extract_a_task in merge_task.upstream_list
        assert extract_b_task in merge_task.upstream_list
        assert len(merge_task.upstream_list) == 2

    def test_circular_dependency_raises_error(self):
        """Test that circular dependencies raise error."""
        from dags.factory.dag_builder import DAGBuilder, CircularDependencyError

        config_with_cycle = {
            "dag_id": "circular_dag",
            "description": "DAG with circular dependencies",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tasks": [
                {
                    "task_id": "task_a",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo A"},
                    "dependencies": ["task_c"],
                },
                {
                    "task_id": "task_b",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo B"},
                    "dependencies": ["task_a"],
                },
                {
                    "task_id": "task_c",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo C"},
                    "dependencies": ["task_b"],
                },
            ],
        }

        builder = DAGBuilder()

        with pytest.raises(CircularDependencyError):
            builder.build_dag(config_with_cycle)

    def test_non_existent_dependency_raises_error(self):
        """Test that referencing non-existent task raises error."""
        from dags.factory.dag_builder import DAGBuilder, DAGBuildError

        config_bad_dep = {
            "dag_id": "bad_dependency_dag",
            "description": "DAG with non-existent dependency",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tasks": [
                {
                    "task_id": "task_a",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo A"},
                    "dependencies": ["non_existent_task"],
                }
            ],
        }

        builder = DAGBuilder()

        with pytest.raises(DAGBuildError) as exc_info:
            builder.build_dag(config_bad_dep)

        assert "non_existent_task" in str(exc_info.value)

    def test_default_args_applied_to_tasks(self):
        """Test that default_args from config are applied to all tasks."""
        from dags.factory.dag_builder import DAGBuilder

        config = {
            "dag_id": "default_args_dag",
            "description": "DAG with default args",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {
                "owner": "test_owner",
                "retries": 5,
                "retry_delay_minutes": 10,
            },
            "tasks": [
                {
                    "task_id": "task_a",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo A"},
                }
            ],
        }

        builder = DAGBuilder()
        dag = builder.build_dag(config)

        task = dag.task_dict["task_a"]
        assert task.owner == "test_owner"
        assert task.retries == 5

    def test_task_params_override_defaults(self):
        """Test that task-specific params override default_args."""
        from dags.factory.dag_builder import DAGBuilder

        config = {
            "dag_id": "override_dag",
            "description": "DAG with param overrides",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "default_owner", "retries": 2},
            "tasks": [
                {
                    "task_id": "task_with_override",
                    "operator": "BashOperator",
                    "params": {
                        "bash_command": "echo A",
                        "retries": 10,  # Override default
                    },
                }
            ],
        }

        builder = DAGBuilder()
        dag = builder.build_dag(config)

        task = dag.task_dict["task_with_override"]
        assert task.retries == 10  # Should use task-specific value

    def test_build_dag_with_no_dependencies(self):
        """Test building DAG where all tasks are independent."""
        from dags.factory.dag_builder import DAGBuilder

        config = {
            "dag_id": "parallel_dag",
            "description": "DAG with parallel tasks",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tasks": [
                {
                    "task_id": f"task_{i}",
                    "operator": "BashOperator",
                    "params": {"bash_command": f"echo 'Task {i}'"},
                }
                for i in range(5)
            ],
        }

        builder = DAGBuilder()
        dag = builder.build_dag(config)

        # All tasks should have no dependencies
        for task in dag.tasks:
            assert len(task.upstream_list) == 0
            assert len(task.downstream_list) == 0

    def test_build_dag_from_file(self, mock_dag_configs_dir):
        """Test building DAG from JSON file."""
        from dags.factory.dag_builder import DAGBuilder

        config_file = mock_dag_configs_dir / "simple_etl.json"
        builder = DAGBuilder()
        dag = builder.build_dag_from_file(str(config_file))

        assert isinstance(dag, DAG)
        assert dag.dag_id == "simple_etl"

    def test_build_dag_with_invalid_operator_raises_error(self):
        """Test that invalid operator name raises error."""
        from dags.factory.dag_builder import DAGBuilder, DAGBuildError

        config = {
            "dag_id": "invalid_operator_dag",
            "description": "DAG with invalid operator",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tasks": [
                {
                    "task_id": "task_a",
                    "operator": "NonExistentOperator",
                    "params": {},
                }
            ],
        }

        builder = DAGBuilder()

        with pytest.raises(DAGBuildError):
            builder.build_dag(config)

    def test_dag_tags_applied_from_config(self):
        """Test that DAG tags are applied from config."""
        from dags.factory.dag_builder import DAGBuilder

        config = {
            "dag_id": "tagged_dag",
            "description": "DAG with tags",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tags": ["etl", "demo", "example"],
            "tasks": [
                {
                    "task_id": "task_a",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo A"},
                }
            ],
        }

        builder = DAGBuilder()
        dag = builder.build_dag(config)

        assert "etl" in dag.tags
        assert "demo" in dag.tags
        assert "example" in dag.tags

    def test_dag_catchup_setting_applied(self):
        """Test that catchup setting is applied from config."""
        from dags.factory.dag_builder import DAGBuilder

        config = {
            "dag_id": "no_catchup_dag",
            "description": "DAG with catchup disabled",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "catchup": False,
            "tasks": [
                {
                    "task_id": "task_a",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo A"},
                }
            ],
        }

        builder = DAGBuilder()
        dag = builder.build_dag(config)

        assert dag.catchup is False

    def test_error_message_includes_dag_id_on_failure(self):
        """Test that error messages include DAG ID for debugging."""
        from dags.factory.dag_builder import DAGBuilder, DAGBuildError

        config = {
            "dag_id": "error_test_dag",
            "description": "DAG that will fail",
            "schedule_interval": "0 2 * * *",
            "start_date": "2024-01-01",
            "default_args": {"owner": "airflow", "retries": 1},
            "tasks": [
                {
                    "task_id": "task_a",
                    "operator": "BashOperator",
                    "params": {"bash_command": "echo A"},
                    "dependencies": ["non_existent"],
                }
            ],
        }

        builder = DAGBuilder()

        with pytest.raises(DAGBuildError) as exc_info:
            builder.build_dag(config)

        assert "error_test_dag" in str(exc_info.value)
