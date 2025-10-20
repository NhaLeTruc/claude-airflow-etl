"""
Spark Kubernetes Operator for Apache Airflow.

Custom operator for submitting Spark jobs to a Kubernetes cluster.
"""

from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from src.hooks.spark_hook import SparkHook, SparkJobStatus
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SparkKubernetesOperator(BaseOperator):
    """
    Operator for submitting Spark applications to a Kubernetes cluster.

    :param application: Path to the Spark application (Python or JAR file)
    :param namespace: Kubernetes namespace for Spark resources
    :param application_args: Arguments to pass to the application
    :param conf: Spark configuration properties
    :param name: Application name
    :param kubernetes_service_account: Kubernetes service account name
    :param image: Docker image for Spark driver and executors
    :param driver_pod_template: Path to driver pod template YAML
    :param executor_pod_template: Path to executor pod template YAML
    :param executor_pod_cleanup_policy: Pod cleanup policy ('OnSuccess', 'OnFailure', 'Never')
    :param driver_memory: Driver memory (e.g., '1g')
    :param driver_cores: Number of driver cores
    :param executor_memory: Executor memory (e.g., '2g')
    :param executor_cores: Number of executor cores
    :param num_executors: Number of executors
    :param verbose: Enable verbose spark-submit output
    :param conn_id: Airflow connection ID for Spark
    :param kubernetes_master: Kubernetes API server URL (e.g., 'k8s://https://api.k8s.example.com')
    """

    template_fields = ("application", "application_args", "namespace", "image", "conf", "name")
    template_ext = (".py", ".jar")
    ui_color = "#326ce5"  # Blue for Kubernetes

    @apply_defaults
    def __init__(
        self,
        *,
        application: str,
        namespace: str,
        application_args: Optional[List[str]] = None,
        conf: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        kubernetes_service_account: Optional[str] = None,
        image: Optional[str] = None,
        driver_pod_template: Optional[str] = None,
        executor_pod_template: Optional[str] = None,
        executor_pod_cleanup_policy: str = "OnSuccess",
        driver_memory: Optional[str] = None,
        driver_cores: Optional[str] = None,
        executor_memory: Optional[str] = None,
        executor_cores: Optional[str] = None,
        num_executors: Optional[str] = None,
        verbose: bool = False,
        conn_id: str = "spark_default",
        kubernetes_master: str = "k8s://https://kubernetes.default.svc",
        **kwargs,
    ):
        """Initialize SparkKubernetesOperator."""
        super().__init__(**kwargs)

        # Validate required parameters
        if not application:
            raise ValueError("application parameter is required")
        if not namespace:
            raise TypeError("namespace parameter is required")

        # Validate cleanup policy
        valid_policies = ["OnSuccess", "OnFailure", "Never"]
        if executor_pod_cleanup_policy not in valid_policies:
            raise ValueError(
                f"Invalid executor_pod_cleanup_policy: {executor_pod_cleanup_policy}. "
                f"Must be one of: {', '.join(valid_policies)}"
            )

        self.application = application
        self.namespace = namespace
        self.application_args = application_args or []
        self.conf = conf or {}
        self.name = name or f"spark-k8s-{self.task_id}"
        self.kubernetes_service_account = kubernetes_service_account
        self.image = image
        self.driver_pod_template = driver_pod_template
        self.executor_pod_template = executor_pod_template
        self.executor_pod_cleanup_policy = executor_pod_cleanup_policy
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.verbose = verbose
        self.conn_id = conn_id
        self.kubernetes_master = kubernetes_master

        self._job_id: Optional[str] = None
        self._hook: Optional[SparkHook] = None

    def execute(self, context: Dict[str, Any]) -> str:
        """
        Execute the Spark Kubernetes job.

        :param context: Airflow task context
        :return: Job ID
        """
        logger.info(f"Executing Spark Kubernetes job: {self.name}")
        logger.info(f"Application: {self.application}")
        logger.info(f"Namespace: {self.namespace}")

        # Initialize hook
        self._hook = SparkHook(conn_id=self.conn_id, verbose=self.verbose)

        # Build Kubernetes-specific configuration
        k8s_conf = self.conf.copy()

        # Namespace
        k8s_conf["spark.kubernetes.namespace"] = self.namespace

        # Service account
        if self.kubernetes_service_account:
            k8s_conf["spark.kubernetes.authenticate.driver.serviceAccountName"] = (
                self.kubernetes_service_account
            )

        # Container image
        if self.image:
            k8s_conf["spark.kubernetes.container.image"] = self.image

        # Pod templates
        if self.driver_pod_template:
            k8s_conf["spark.kubernetes.driver.podTemplateFile"] = self.driver_pod_template
        if self.executor_pod_template:
            k8s_conf["spark.kubernetes.executor.podTemplateFile"] = self.executor_pod_template

        # Cleanup policy
        k8s_conf["spark.kubernetes.executor.deleteOnTermination"] = (
            "true" if self.executor_pod_cleanup_policy in ["OnSuccess", "OnFailure"] else "false"
        )

        try:
            # Submit job with Kubernetes master
            self._job_id = self._hook.submit_job(
                application=self.application,
                master=self.kubernetes_master,
                deploy_mode="cluster",  # K8s typically uses cluster mode
                name=self.name,
                conf=k8s_conf,
                application_args=self.application_args,
                driver_memory=self.driver_memory,
                driver_cores=self.driver_cores,
                executor_memory=self.executor_memory,
                executor_cores=self.executor_cores,
                num_executors=self.num_executors,
            )

            logger.info(f"Spark Kubernetes job submitted. Job ID: {self._job_id}")

            # Wait for completion
            success = self._hook.wait_for_completion(self._job_id, timeout=None, poll_interval=5)

            if not success:
                status = self._hook.get_job_status(self._job_id)
                error_msg = f"Spark Kubernetes job {self._job_id} failed with status: {status.value}"
                logger.error(error_msg)

                # Try to get logs
                logs = self._hook.get_job_logs(self._job_id)
                if logs:
                    logger.error(f"Job logs:\n{logs}")

                raise AirflowException(error_msg)

            logger.info(f"Spark Kubernetes job {self._job_id} completed successfully")

            # Push job ID to XCom
            context["task_instance"].xcom_push(key="k8s_spark_job_id", value=self._job_id)

            return self._job_id

        except Exception as e:
            logger.error(f"Error executing Spark Kubernetes job: {str(e)}")
            raise AirflowException(f"Spark Kubernetes job execution failed: {str(e)}") from e

    def on_kill(self):
        """Handle task kill by terminating Spark job."""
        if self._hook and self._job_id:
            logger.warning(f"Task killed, terminating Spark Kubernetes job {self._job_id}")
            self._hook.kill_job(self._job_id)