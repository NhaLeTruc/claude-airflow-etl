# Spark Standalone Cluster Setup

This directory contains Docker configuration for running a Spark Standalone cluster locally for testing Airflow Spark operators.

## Quick Start

```bash
# Start Spark cluster
docker compose -f docker/spark/docker-compose.spark.yml up -d

# Check cluster status
docker compose -f docker/spark/docker-compose.spark.yml ps

# Access Spark Master UI
open http://localhost:8090

# Access Worker UIs
open http://localhost:8091  # Worker 1
open http://localhost:8092  # Worker 2

# Stop cluster
docker compose -f docker/spark/docker-compose.spark.yml down
```

## Architecture

- **Spark Master**: Coordinates job execution
  - UI: http://localhost:8090
  - Master URL: spark://spark-master:7077

- **Spark Worker 1**: Executes tasks
  - 2 cores, 2GB memory
  - UI: http://localhost:8091

- **Spark Worker 2**: Executes tasks
  - 2 cores, 2GB memory
  - UI: http://localhost:8092

## Integration with Airflow

The Spark cluster is connected to the same Docker network as Airflow (`airflow-network`), allowing Airflow DAGs to submit jobs directly.

### Airflow Connection Setup

Create an Airflow connection for Spark:

```bash
docker exec airflow-scheduler airflow connections add spark_standalone \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy_mode": "client", "spark_binary": "spark-submit"}'
```

### Example Usage in DAG

```python
from src.operators.spark.standalone_operator import SparkStandaloneOperator

spark_task = SparkStandaloneOperator(
    task_id='run_spark_job',
    application='/opt/spark/apps/word_count.py',
    master='spark://spark-master:7077',
    conn_id='spark_standalone',
    dag=dag,
)
```

## Testing

```bash
# Submit a test job directly
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/word_count.py

# View job in Spark UI
open http://localhost:8090
```

## Troubleshooting

### Workers not connecting to master

```bash
# Check logs
docker compose -f docker/spark/docker-compose.spark.yml logs spark-master
docker compose -f docker/spark/docker-compose.spark.yml logs spark-worker-1

# Restart cluster
docker compose -f docker/spark/docker-compose.spark.yml restart
```

### Port conflicts

If port 8090 is in use, modify `docker-compose.spark.yml`:

```yaml
ports:
  - "9090:8080"  # Use port 9090 instead
```

## Resource Configuration

Modify worker resources in `docker-compose.spark.yml`:

```yaml
environment:
  - SPARK_WORKER_CORES=4      # Increase cores
  - SPARK_WORKER_MEMORY=4g    # Increase memory
```

## Clean Up

```bash
# Remove all containers and volumes
docker compose -f docker/spark/docker-compose.spark.yml down -v

# Remove images
docker rmi $(docker images -q apache/spark:3.5.0-python3)
```