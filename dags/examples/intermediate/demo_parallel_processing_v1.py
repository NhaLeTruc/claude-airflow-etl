"""
Parallel Processing Pattern - Intermediate Example DAG

This DAG demonstrates parallel task execution with fan-out and fan-in patterns.
Shows how to process multiple independent datasets concurrently and then
aggregate results in a final task.

Pattern Demonstrated:
- Fan-out: Single task triggers multiple parallel tasks
- Fan-in: Multiple parallel tasks converge to single aggregation task
- Task groups for logical organization
- Dynamic task generation for scalability

Use Case:
Process sales data for multiple product categories in parallel, then aggregate
results into a summary report. Demonstrates efficiency gains from parallelization.

Learning Objectives:
- Understand Airflow's parallel execution capabilities
- Learn fan-out/fan-in pattern implementation
- See how to use task groups for organization
- Understand when to parallelize vs. serialize tasks

Constitutional Compliance:
- Principle I: DAG-First Development (proper naming, clear structure)
- Principle V: Observability (structured logging, execution tracking)
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)


# Default arguments
default_args = {
    "owner": "data-engineering@example.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def extract_category_data(category: str, **context):
    """
    Extract sales data for a specific product category.

    Args:
        category: Product category to extract (e.g., 'Electronics', 'Clothing')
        **context: Airflow context dictionary

    Returns:
        dict: Extraction statistics
    """
    execution_date = context["execution_date"]

    logger.info(f"Extracting sales data for category: {category}")
    logger.info(f"Execution date: {execution_date}")

    # Simulate extraction logic
    # In real implementation, this would query the warehouse
    record_count = hash(category) % 1000 + 100  # Simulate varying record counts

    result = {
        "category": category,
        "records_extracted": record_count,
        "extraction_timestamp": datetime.now().isoformat(),
        "execution_date": execution_date.isoformat(),
    }

    logger.info(f"Extracted {record_count} records for {category}")

    # Push to XCom for downstream tasks
    context["task_instance"].xcom_push(key=f"extract_{category}", value=result)

    return result


def transform_category_data(category: str, **context):
    """
    Transform sales data for a specific product category.

    Args:
        category: Product category to transform
        **context: Airflow context dictionary

    Returns:
        dict: Transformation statistics
    """
    task_instance = context["task_instance"]

    # Pull extraction results from XCom
    extract_result = task_instance.xcom_pull(
        task_ids=f"extract_group.extract_{category}", key=f"extract_{category}"
    )

    if not extract_result:
        logger.warning(f"No extraction data found for {category}")
        return {"category": category, "status": "skipped"}

    logger.info(f"Transforming {extract_result['records_extracted']} records for {category}")

    # Simulate transformation logic
    transformed_count = extract_result["records_extracted"]
    invalid_count = int(transformed_count * 0.02)  # 2% invalid records
    valid_count = transformed_count - invalid_count

    result = {
        "category": category,
        "records_transformed": valid_count,
        "records_invalid": invalid_count,
        "transformation_timestamp": datetime.now().isoformat(),
    }

    logger.info(
        f"Transformed {valid_count} valid records for {category} "
        f"({invalid_count} invalid records filtered)"
    )

    # Push to XCom for aggregation
    task_instance.xcom_push(key=f"transform_{category}", value=result)

    return result


def load_category_data(category: str, **context):
    """
    Load transformed sales data for a specific product category.

    Args:
        category: Product category to load
        **context: Airflow context dictionary

    Returns:
        dict: Load statistics
    """
    task_instance = context["task_instance"]

    # Pull transformation results from XCom
    transform_result = task_instance.xcom_pull(
        task_ids=f"transform_group.transform_{category}", key=f"transform_{category}"
    )

    if not transform_result or transform_result.get("status") == "skipped":
        logger.warning(f"No transformation data found for {category}")
        return {"category": category, "status": "skipped"}

    records_to_load = transform_result["records_transformed"]
    logger.info(f"Loading {records_to_load} records for {category}")

    # Simulate load logic
    # In real implementation, this would insert into warehouse

    result = {
        "category": category,
        "records_loaded": records_to_load,
        "load_timestamp": datetime.now().isoformat(),
        "status": "success",
    }

    logger.info(f"Successfully loaded {records_to_load} records for {category}")

    # Push to XCom for aggregation
    task_instance.xcom_push(key=f"load_{category}", value=result)

    return result


def aggregate_results(**context):
    """
    Aggregate results from all parallel category processing tasks.

    This is the fan-in task that collects results from all parallel branches.

    Args:
        **context: Airflow context dictionary

    Returns:
        dict: Aggregated statistics across all categories
    """
    task_instance = context["task_instance"]

    # Define categories (should match parallel tasks)
    categories = ["Electronics", "Clothing", "Food", "Books"]

    total_extracted = 0
    total_transformed = 0
    total_loaded = 0
    total_invalid = 0
    category_stats = []

    # Pull results from all parallel load tasks
    for category in categories:
        load_result = task_instance.xcom_pull(
            task_ids=f"load_group.load_{category}", key=f"load_{category}"
        )

        if load_result and load_result.get("status") == "success":
            # Also get transform stats for invalid count
            transform_result = task_instance.xcom_pull(
                task_ids=f"transform_group.transform_{category}", key=f"transform_{category}"
            )

            extract_result = task_instance.xcom_pull(
                task_ids=f"extract_group.extract_{category}", key=f"extract_{category}"
            )

            if extract_result:
                total_extracted += extract_result.get("records_extracted", 0)

            if transform_result:
                total_transformed += transform_result.get("records_transformed", 0)
                total_invalid += transform_result.get("records_invalid", 0)

            total_loaded += load_result.get("records_loaded", 0)

            category_stats.append(
                {
                    "category": category,
                    "extracted": (
                        extract_result.get("records_extracted", 0) if extract_result else 0
                    ),
                    "loaded": load_result.get("records_loaded", 0),
                }
            )

    aggregated_result = {
        "total_categories_processed": len(category_stats),
        "total_records_extracted": total_extracted,
        "total_records_transformed": total_transformed,
        "total_records_loaded": total_loaded,
        "total_invalid_records": total_invalid,
        "data_quality_rate": (
            (total_transformed / total_extracted * 100) if total_extracted > 0 else 0
        ),
        "category_breakdown": category_stats,
        "aggregation_timestamp": datetime.now().isoformat(),
    }

    logger.info("=" * 80)
    logger.info("PARALLEL PROCESSING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Categories Processed: {aggregated_result['total_categories_processed']}")
    logger.info(f"Total Extracted: {aggregated_result['total_records_extracted']}")
    logger.info(f"Total Loaded: {aggregated_result['total_records_loaded']}")
    logger.info(f"Total Invalid: {aggregated_result['total_invalid_records']}")
    logger.info(f"Data Quality Rate: {aggregated_result['data_quality_rate']:.2f}%")
    logger.info("=" * 80)

    return aggregated_result


def send_completion_notification(**context):
    """
    Send notification with aggregation results.

    Args:
        **context: Airflow context dictionary
    """
    task_instance = context["task_instance"]

    # Pull aggregated results
    agg_result = task_instance.xcom_pull(task_ids="aggregate_results")

    if not agg_result:
        logger.warning("No aggregated results found for notification")
        return

    message = f"""
    Parallel ETL Processing Completed

    Categories Processed: {agg_result['total_categories_processed']}
    Records Loaded: {agg_result['total_records_loaded']:,}
    Data Quality Rate: {agg_result['data_quality_rate']:.2f}%

    Execution Date: {context['execution_date']}
    """

    logger.info(f"Sending completion notification:\n{message}")

    # In real implementation, this would send actual notification
    # For demo, we just log the message


# Define the DAG
with DAG(
    dag_id="demo_parallel_processing_v1",
    default_args=default_args,
    description="Demonstrates parallel task execution with fan-out/fan-in pattern",
    schedule=None,  # Manual trigger for demo
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["intermediate", "parallel", "pattern", "demo"],
    doc_md=__doc__,
) as dag:
    # Start task
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logger.info("Starting parallel processing pipeline"),
    )

    # Define product categories to process in parallel
    categories = ["Electronics", "Clothing", "Food", "Books"]

    # FAN-OUT: Extract tasks in parallel (one per category)
    with TaskGroup(group_id="extract_group") as extract_group:
        for category in categories:
            PythonOperator(
                task_id=f"extract_{category}",
                python_callable=extract_category_data,
                op_kwargs={"category": category},
                doc_md=f"Extract sales data for {category} category",
            )

    # Transform tasks in parallel (one per category)
    with TaskGroup(group_id="transform_group") as transform_group:
        for category in categories:
            PythonOperator(
                task_id=f"transform_{category}",
                python_callable=transform_category_data,
                op_kwargs={"category": category},
                doc_md=f"Transform sales data for {category} category",
            )

    # Load tasks in parallel (one per category)
    with TaskGroup(group_id="load_group") as load_group:
        for category in categories:
            PythonOperator(
                task_id=f"load_{category}",
                python_callable=load_category_data,
                op_kwargs={"category": category},
                doc_md=f"Load sales data for {category} category",
            )

    # FAN-IN: Aggregate results from all parallel tasks
    aggregate = PythonOperator(
        task_id="aggregate_results",
        python_callable=aggregate_results,
        doc_md="Aggregate results from all parallel category processing tasks",
    )

    # Send notification
    notify = PythonOperator(
        task_id="send_notification",
        python_callable=send_completion_notification,
        doc_md="Send completion notification with aggregated statistics",
    )

    # End task
    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logger.info("Parallel processing pipeline completed"),
    )

    # Define task dependencies
    # Fan-out pattern: start → extract (parallel) → transform (parallel) → load (parallel)
    # Fan-in pattern: load (parallel) → aggregate → notify → end
    start >> extract_group >> transform_group >> load_group >> aggregate >> notify >> end
