"""Utility functions for working with Dataproc clusters in the Platform project."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    InstanceFlexibilityPolicy,
    PreemptibilityType,
)
from airflow.utils.context import Context
from google.api_core.exceptions import NotFound as GCPNotFound
from google.cloud.dataproc_v1 import JobReference
from google.cloud.dataproc_v1.types import DiskConfig, NodeInitializationAction
from google.cloud.dataproc_v1.types.jobs import Job, JobPlacement, PySparkJob, SparkJob
from pydantic import BaseModel, model_validator

from orchestration.models.secret import Secret, SecretInitAction, Secrets
from orchestration.utils import convert_params_to_hydra_positional_arg, random_id, resource_name
from orchestration.utils.common import GCP_PROJECT_PLATFORM, GCP_REGION, GCP_SERVICE_ACCOUNT, GCP_ZONE
from orchestration.utils.labels import Labels

if TYPE_CHECKING:
    from typing import Any, Self


class CustomClusterConfig(BaseModel):
    """Dataproc cluster configuration class.

    Includes defaults tailored to our cluster needs.
    """

    project_id: str = GCP_PROJECT_PLATFORM
    """Google cloud project ID in which to create the cluster. Default is GCP_PROJECT_PLATFORM."""
    zone: str | None = GCP_ZONE
    """Google cloud zone in which to create the cluster. Default is GCP_ZONE."""

    custom_image: str | None = None
    """Custom Dataproc image to use for the cluster."""
    custom_image_project_id: str | None = None
    """Google cloud project ID of the custom image."""
    custom_image_family: str | None = None
    """Image family for the custom dataproc image."""
    image_version: str | None = "2.2"
    """The version of software inside the cluster."""

    autoscaling_policy: str | None = None
    """Autoscaling policy resource. Project ID and region will be automatically
        added when the class is instantiated if not provided."""

    num_masters: int = 1
    """The number of master nodes to spin up. Default is 1."""
    master_machine_type: str = "n1-highmem-16"
    """GCE machine type to use for master nodes. Default is n1-highmem-16."""
    master_disk_type: str = "pd-ssd"
    """The disk type to use for master nodes. Default is pd-ssd."""
    master_disk_size: int = 512
    """The disk size in GB to use for master nodes. Default is 500."""
    master_accelerator_type: str | None = None
    """The GPU type to use for master nodes."""
    master_accelerator_count: int | None = None
    """The number of GPUs to use for master nodes."""

    num_workers: int | None = 2
    """The number of worker nodes in the cluster (0 for single-node mode).
        Default is 2."""
    min_num_workers: int | None = None
    """The minimum number of primary worker nodes in the cluster.
        If more than ``min_num_workers`` VMs are created out of ``num_workers``,
        the failed VMs will be deleted, cluster is resized to available VMs and
        set to RUNNING.
        If created VMs are less than ``min_num_workers``, the cluster is placed
        in ERROR state. The failed VMs are not deleted.
    """
    num_preemptible_workers: int = 0
    """The number of instances in the instance group as secondary workers.
        Default is 0.
    """
    worker_machine_type: str = "n1-standard-4"
    """GCE machine type to use for worker nodes. Default is n1-standard-4."""
    worker_disk_type: str = "pd-ssd"
    """The disk type to use for worker nodes. Default is pd-ssd."""
    worker_disk_size: int = 2048
    """The disk size to use for worker nodes. Default is 2048."""
    worker_accelerator_type: str | None = None
    """The GPU type to use for worker nodes."""
    worker_accelerator_count: int | None = None
    """The number of GPUs to use for worker nodes."""

    secondary_worker_machine_type: str | None = None
    """GCE machine type to use for secondary worker nodes. Default is same as worker_machine_type."""
    secondary_worker_disk_type: str | None = None
    """The disk type to use for secondary workers. Default is same as worker_disk_type."""
    secondary_worker_disk_size: int | None = None
    """The disk size to use for secondary workers. Default is same as worker_disk_size."""
    secondary_worker_instance_flexibility_policy: InstanceFlexibilityPolicy | None = None
    """Instance flexibility Policy allowing a mixture of VM shapes and
        provisioning models."""
    secondary_worker_accelerator_type: str | None = None
    """The GPU type to use for secondary workers."""
    secondary_worker_accelerator_count: int | None = None
    """The number of GPUs to use for secondary workers."""

    driver_pool_size: int = 0
    """The number of driver nodes in node group. Default is 0."""
    driver_pool_id: str | None = None
    """The ID for the driver pool."""

    idle_delete_ttl: int | None = 7200
    """Delete the cluster after this many seconds of inactivity. Default is 7200
        (2 hours)."""
    auto_delete_time: datetime | None = None
    """Delete the cluster at this time."""
    auto_delete_ttl: int | None = None
    """Delete the cluster after this many seconds."""
    customer_managed_key: str | None = None
    """The customer managed key to use for disk encryption."""
    enable_component_gateway: bool | None = True
    """Provides access to the web interfaces of default and selected optional
        components on the cluster. Default is True."""

    network_uri: str | None = None
    """The network uri to be used for machine communication, cannot be
        specified with subnetwork_uri"""
    subnetwork_uri: str | None = None
    """The subnetwork uri to be used for machine communication, cannot be
        specified with network_uri"""
    internal_ip_only: bool | None = None
    """If true, all instances in the cluster will only have internal IP addresses.
        This can only be enabled for subnetwork enabled networks"""
    optional_components: list[str] | None = None
    """List of optional cluster components, for more info see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#Component"""
    preemptibility: str = PreemptibilityType.PREEMPTIBLE.value
    """Type of preemptibility to use for secondary workers. See:
        https://cloud.google.com/dataproc/docs/reference/rpc/
        Default is PreemptibilityType.PREEMPTIBLE.value.
    """

    tags: list[str] | None = None
    """The list of tags to add to all instances. Keep in mind labels are not
        specified here but in the cluster creation operator"""
    storage_bucket: str | None = None
    """The Cloud Storage bucket to use, if None Dataproc will create one."""
    metadata: dict | None = None
    """Dict of GCE metadata entries to add to all instances."""
    properties: dict | None = None
    """Dict of properties to set on config files."""

    init_actions_uris: list[str] | None = None
    """List of GCS URIs of initialization scripts."""
    init_action_timeout: str = "10m"
    """Timeout for initialization actions. Default is 10 minutes."""

    service_account: str | None = GCP_SERVICE_ACCOUNT
    """The service account to use for the cluster. Default is GCP_SERVICE_ACCOUNT."""
    service_account_scopes: list[str] | None = None
    """The scopes to use for the cluster."""

    secret_map: dict[str, str] | None = None
    """The dict of secrets where the `value` is the `secret id` from GoogleSecretManager
         and the `key` is the environment variable name that the value of the secret
         will be stored in on the cluster. By default the latest version of the secret will be used."""
    secret_init_action_uri: str | None = None
    """The URI of the init action script that will handle the secret injection. Default to None."""

    @model_validator(mode="after")
    def validate_secret_config(self) -> Self:
        """If secret_map is set and not empty, secret_init_action_uri must be set."""
        if self.secret_map and not self.secret_init_action_uri:
            raise ValueError("secret_init_action_uri must be set if secret_map is set")
        return self

    def model_post_init(self, _: Any) -> None:
        if isinstance(self.autoscaling_policy, str) and "/" not in self.autoscaling_policy:
            zone = self.zone or GCP_ZONE
            region = zone.rsplit("-", 1)[0]
            ap = f"projects/{self.project_id}/regions/{region}/autoscalingPolicies/{self.autoscaling_policy}"
            self.autoscaling_policy = ap

    def _update_c4_machine_disk_config(self, disk_config: DiskConfig) -> DiskConfig:
        """Update the disk config with the right values for c4 machine types."""
        disk_config.boot_disk_type = "hyperdisk-balanced"
        disk_config.boot_disk_provisioned_iops = 6_000
        disk_config.boot_disk_provisioned_throughput = 500
        return disk_config

    def _create_secondary_worker_disk_config(self) -> DiskConfig:
        """Override the disk config with the values for the secondary workers if they are set."""
        disk_config = DiskConfig()
        disk_config.boot_disk_size_gb = self.secondary_worker_disk_size or disk_config.boot_disk_size_gb
        disk_config.boot_disk_type = self.secondary_worker_disk_type or disk_config.boot_disk_type
        return disk_config

    def create_cluster(self) -> dict[str, Any]:
        """Create a Dataproc cluster from the configuration.

        Returns:
            ClusterConfig: The Dataproc cluster.
        """
        exclude_fields = {
            "secondary_worker_disk_type",
            "secondary_worker_disk_size",
            "secondary_worker_machine_type",
            "secret_map",
            "secret_init_action_uri",
        }
        config = ClusterGenerator(**self.model_dump(exclude=exclude_fields)).make()

        # Ensure that the c4- machine types have the right disk config
        # TODO: Refactor once we are sure we need the c4- machine types

        if self.worker_machine_type.startswith("c4-"):
            dc = DiskConfig(**config["worker_config"]["disk_config"])
            dc = self._update_c4_machine_disk_config(dc)
            config["worker_config"]["disk_config"] = dc
        if self.master_machine_type.startswith("c4-"):
            dc = DiskConfig(**config["master_config"]["disk_config"])
            dc = self._update_c4_machine_disk_config(dc)
            config["master_config"]["disk_config"] = dc
        # By default the secondary workers have the same disk config as the primary workers, but we want to be able to set it independently
        if self.secondary_worker_machine_type:
            config["secondary_worker_config"]["machine_type_uri"] = self.secondary_worker_machine_type
        if self.secondary_worker_disk_size or self.secondary_worker_disk_type:
            dc = self._create_secondary_worker_disk_config()
            config["secondary_worker_config"]["disk_config"] = dc

        return config


class ClusterDefinition(NamedTuple):
    """Cluster definition.

    This class is used to define the cluster configuration for a step in the
    pipeline. It contains the cluster type and configuration.
    """

    cluster_type: str
    """The type of the cluster."""
    config: dict[str, Any]
    """The configuration dict for the cluster. See
        `src.orchestration.utils.dataproc.ClusterConfig`."""

    @property
    def cluster_name(self) -> str:
        """Returns the resource name for this cluster definition."""
        return resource_name(self.cluster_type)

    @property
    def cluster_config(self) -> CustomClusterConfig:
        """Returns the CustomClusterConfig object for this cluster definition."""
        return CustomClusterConfig(**self.config)


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
        cluster_config (CustomClusterConfig): The cluster configuration.
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
        cluster_config: CustomClusterConfig,
        labels: Labels | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.region = region
        self._cluster_config = cluster_config
        self.labels = labels or Labels()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        self.cluster_config = cluster_config.create_cluster()

        super().__init__(
            cluster_name=cluster_name,
            region=self.region,
            project_id=self.project_id,
            # Apparently the actual `self.cluster_config` is
            # `dataproc_v1.types.cluster.ClusterConfig` and not `dataproc_v1.types.cluster.Cluster`,
            # but passing both types seem to work fine anyway???
            cluster_config=self.cluster_config,  # type: ignore
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
        secret_action = self._prepare_secret_init_action()
        self._patch_cluster_init_actions([secret_action] if secret_action else [])
        return super().execute(context)

    def _prepare_secret_init_action(self) -> NodeInitializationAction | None:
        """Prepare the secret init action if secrets exist."""
        if not self._cluster_config.secret_map:
            return None
        if not self._cluster_config.secret_init_action_uri:
            raise AirflowException("secret_init_action_uri must be set if secret_map is set")
        secrets = Secrets(
            mapping={
                env_var: Secret(secret_id=secret_name, project_id=self.project_id)
                for env_var, secret_name in self._cluster_config.secret_map.items()
            }
        )
        init_action = SecretInitAction(
            secrets=secrets,
            init_action_uri=self._cluster_config.secret_init_action_uri,
        )
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        return init_action.push_to_gcs(gcs_hook=hook)

    def _patch_cluster_init_actions(self, init_actions: list[NodeInitializationAction] | None) -> None:
        """Patch in place the cluster init actions."""
        if not init_actions:
            return
        self.log.info(f"Patching cluster init actions with {init_actions}")
        self.log.debug(f"Current cluster config: {self.cluster_config}")
        self.cluster_config["initialization_actions"].extend(init_actions)  # type: ignore


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
