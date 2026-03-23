"""Utility functions for working with Dataproc clusters in the Platform project."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.context import Context
from google.api_core.exceptions import NotFound as GCPNotFound
from google.cloud.dataproc_v1 import JobReference
from google.cloud.dataproc_v1.types.jobs import Job, JobPlacement, PySparkJob, SparkJob

from orchestration.models.infrastructure.dataproc import ClusterConfig
from orchestration.utils import convert_params_to_hydra_positional_arg, random_id
from orchestration.utils.common import GCP_PROJECT_PLATFORM, GCP_REGION
from orchestration.utils.labels import Labels


class CreateClusterOperator(DataprocCreateClusterOperator):
    """Create a new Dataproc cluster.

    This class wraps the original DataprocCreateClusterOperator to provide some
    tooling around it. Already existing clusters will be used.

    For more information on how to use this operator, take a look at
    `the guide <https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html#howto-operator-dataproccreateclusteroperator>`_.

    Args:
        project_id (str): The ID of the Google cloud project in which to create
            the cluster. Default is `GCP_PROJECT_PLATFORM`. Templated.
        region (str): The region where the cluster will be created. Default is
            `GCP_REGION`. Templated.
        cluster_name (str): The cluster name.
        cluster_config (ClusterConfig): The cluster configuration.
        labels (Labels): The labels assigned to the cluster. Templated.
        gcp_conn_id (str): The connection ID used when connecting to Google Cloud.
        impersonation_chain: (str | Sequence[str | None]) Optional service
            account or chain to impersonate.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "region",
        "cluster_name",
        "labels",
    )

    def __init__(
        self,
        *,
        project_id: str = GCP_PROJECT_PLATFORM,
        region: str = GCP_REGION,
        cluster_name: str,
        cluster_config: ClusterConfig,
        labels: Labels | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.region = region
        self.cluster_config = cluster_config
        self.labels = labels or Labels()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        cluster = cluster_config.create_cluster()

        super().__init__(
            cluster_name=cluster_name,
            region=self.region,
            project_id=self.project_id,
            cluster_config=cluster,
            labels=dict(self.labels),
            use_if_exists=True,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            **kwargs,
        )

    def execute(self, context: Context) -> dict:
        """Execute the operator."""
        # the base operator can only handle dicts, we need to convert back and forth
        labels = Labels({**self.labels})
        labels.add_dag_run_id(context)
        self.labels = dict(labels)
        return super().execute(context)


class SubmitJobOperator(DataprocSubmitJobOperator):
    """Submit a job to a cluster.

    Args:
        project_id (str): The ID of the Google cloud project in which to create
            the cluster. Default is `GCP_PROJECT_PLATFORM`. Templated.
        region (str): The region where the cluster will be created. Default is
            `GCP_REGION`. Templated.
        cluster_name (str): The cluster where to send the job. Templated.
        step_name (str): The name of the step. Templated.
        spark_job (google.cloud.dataproc_v1.types.SparkJob | google.cloud.dataproc_v1.types.PySparkJob):
            The spark/pyspark job that will be submit to run.
        labels (Labels): The labels assigned to the cluster. Templated.
        gcp_conn_id (str): The connection ID used when connecting to Google Cloud.
        impersonation_chain: (str | Sequence[str | None]) Optional service
            account or chain to impersonate.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "region",
        "cluster_name",
        "step_name",
        "labels",
    )

    def __init__(
        self,
        *,
        project_id: str = GCP_PROJECT_PLATFORM,
        region: str = GCP_REGION,
        cluster_name: str,
        step_name: str,
        spark_job: SparkJob | None = None,
        py_spark_job: PySparkJob | None = None,
        labels: Labels | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.step_name = step_name
        self.labels = labels or Labels()
        self.spark_job = spark_job
        self.py_spark_job = py_spark_job
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        # check that either spark_job or py_spark_job but not both are set
        if not bool(spark_job) ^ bool(py_spark_job):
            raise ValueError("provide either spark_job or py_spark_job, but not both")

        # note the job set in here is a `google.cloud.dataproc_v1.types.Job`,
        # which inside contains the spark/pyspark job itself. That one is set
        # in execution time because it needs context.
        super().__init__(
            project_id=self.project_id,
            region=self.region,
            job={},
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            **kwargs,
        )

    def execute(self, context: Context) -> str:
        """Execute the operator."""
        self.labels.add_dag_run_id(context)
        job_id = f"{self.cluster_name}-{self.step_name}-{random_id()}"
        self.job = Job(
            reference=JobReference(project_id=self.project_id, job_id=job_id),
            placement=JobPlacement(cluster_name=self.cluster_name),
            spark_job=self.spark_job,
            pyspark_job=self.py_spark_job,
            labels=self.labels,
        )
        return super().execute(context)


class DeleteClusterOperator(DataprocDeleteClusterOperator):
    def __init__(
        self,
        *,
        region: str = GCP_REGION,
        project_id: str = GCP_PROJECT_PLATFORM,
        **kwargs,
    ) -> None:
        super().__init__(region=region, project_id=project_id, **kwargs)

    def execute(self, context: Context) -> None:
        try:
            super().execute(context)
        except GCPNotFound:
            self.log.warning(f"cluster {self.cluster_name} not found")


class JobBuilder(ABC):
    """Abstract class for Dataproc jobs.

    This class is used to implement jobs for dataproc clusters. It includes some
    utilities that are common to them.

    Any job that is created should implement the `build` method, which should
    return a `SparkJob` or `PySparkJob` object. `build()` will be called by the
    submit job operator.
    """

    @abstractmethod
    def build(self) -> SparkJob | PySparkJob:
        """Build the job.

        Returns:
            SparkJob | PySparkJob: The job to run.
        """

    def render_properties(
        self,
        properties: dict[str, str],
        template_context: dict[str, str],
    ) -> dict[str, str]:
        """Render the properties using the template context.

        Args:
            properties (dict[str, str]): The properties to render.
            template_context (dict[str, str]): The template context to use for
                rendering.

        Returns:
            dict[str, str]: The rendered properties.
        """
        if not properties or not template_context:
            return properties or {}

        result: dict[str, str] = {}
        for property_name, property_value in properties.items():
            for sentinel, value in template_context.items():
                result[property_name] = result.get(property_name, property_value).replace(
                    f"{{{{{sentinel}}}}}",
                    value,
                )
        return result


class ETLJobBuilder(JobBuilder):
    """Class for building ETL jobs."""

    def __init__(
        self,
        jar_uri: str,
        config_uri: str,
        args: list[str],
        properties: dict[str, str] | None = None,
        template_context: dict[str, str] | None = None,
    ) -> None:
        self.jar_uri = jar_uri
        self.config_uri = config_uri
        self.args = args
        self.properties = properties or {}
        self.template_context = template_context or {}
        self.logger = logging.getLogger(__name__)

    def build(self) -> SparkJob:
        """Build a SparkJob that runs an ETL step."""
        rendered_properties = self.render_properties(
            properties=self.properties,
            template_context=self.template_context,
        )

        self.logger.info("spawning etl job")
        self.logger.info(f"jar_uri: {self.jar_uri}")
        self.logger.info(f"config_uri: {self.config_uri}")
        self.logger.info(f"args: {self.args}")
        self.logger.info(f"properties (already rendered): {rendered_properties}")

        return SparkJob(
            main_jar_file_uri=self.jar_uri,
            file_uris=[self.config_uri],
            args=self.args,
            properties=rendered_properties,
        )


class GentropyJobBuilder(JobBuilder):
    """Class for building Gentropy jobs."""

    def __init__(
        self,
        main_python_file_uri: str,
        params: dict[str, str] | None = None,
        properties: dict[str, str] | None = None,
        template_context: dict[str, str] | None = None,
    ) -> None:
        self.main_python_file_uri = main_python_file_uri
        self.params = params or {}
        self.properties = properties or {}
        self.template_context = template_context or {}
        self.logger = logging.getLogger(__name__)

    def build(self) -> PySparkJob:
        """Build a SparkJob that runs a Gentropy step."""
        self.logger.info(f"params: {self.params}")

        args = convert_params_to_hydra_positional_arg(
            params=self.params,
            dataproc=True,
        )
        rendered_properties = self.render_properties(
            properties=self.properties,
            template_context=self.template_context,
        )

        self.logger.info("spawning gentropy job")
        self.logger.info(f"main_python_file_uri: {self.main_python_file_uri}")
        self.logger.info(f"args: {args}")
        self.logger.info(f"properties (already rendered): {rendered_properties}")

        return PySparkJob(
            main_python_file_uri=self.main_python_file_uri,
            args=args,
            properties=rendered_properties,
        )


class PTSJobBuilder(JobBuilder):
    """Class for building PTS dataproc jobs for PySpark PTS tasks."""

    def __init__(
        self,
        main_python_file_uri: str,
        args: list[str],
        config_uri: str,
    ) -> None:
        self.main_python_file_uri = main_python_file_uri
        self.args = args
        self.config_uri = config_uri
        self.logger = logging.getLogger(__name__)

    def build(self) -> PySparkJob:
        """Build a SparkJob that runs a Gentropy step."""
        self.logger.info(f"params: {self.args}")
        self.logger.info("spawning pts job")
        self.logger.info(f"main_python_file_uri: {self.main_python_file_uri}")
        self.logger.info(f"args: {self.args}")

        return PySparkJob(
            main_python_file_uri=self.main_python_file_uri,
            args=self.args,
            file_uris=[self.config_uri],
        )


class CommandJobBuilder(JobBuilder):
    """Class for building Command jobs."""

    def __init__(
        self,
        main_python_file_uri: str,
        command: str,
        properties: dict[str, str] | None = None,
        template_context: dict[str, str] | None = None,
    ) -> None:
        self.main_python_file_uri = main_python_file_uri
        self.command = command
        self.properties = properties or {}
        self.template_context = template_context or {}
        self.logger = logging.getLogger(__name__)

    def build(self) -> PySparkJob:
        """Build a SparkJob that runs a command."""
        rendered_properties = self.render_properties(
            properties=self.properties,
            template_context=self.template_context,
        )

        self.logger.info("spawning command job")
        self.logger.info(f"main_python_file_uri: {self.main_python_file_uri}")
        self.logger.info(f"command: {self.command}")
        self.logger.info(f"properties (already rendered): {rendered_properties}")

        return PySparkJob(
            main_python_file_uri=self.main_python_file_uri,
            args=[self.command],
            properties=rendered_properties,
        )
