"""Models for Dataproc cluster infrastructure specifications.

This module defines Pydantic models used to configure and validate Google Cloud
Dataproc clusters. The models cover the full configuration surface exposed by
the Airflow :class:`~airflow.providers.google.cloud.operators.dataproc.ClusterGenerator`,
including master and worker node sizing, autoscaling, networking, initialisation
actions, and lifecycle management.

Typical usage involves constructing a :class:`ClusterDefinition` (or registering
one in a :class:`ClusterRegistry`) and referencing it from a pipeline step via
an :class:`~orchestration.models.infrastructure.abc.InfrastructurePointer`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    InstanceFlexibilityPolicy,
    PreemptibilityType,
)
from google.cloud.dataproc_v1 import Cluster
from google.cloud.dataproc_v1.types import DiskConfig
from pydantic import BaseModel, field_validator

from orchestration.models.infrastructure.abc import InfrastructureDefinition, InfrastructureRegistry
from orchestration.utils.common import GCP_PROJECT_PLATFORM, GCP_SERVICE_ACCOUNT, GCP_ZONE

INFRASTRUCTURE = "DATAPROC_CLUSTER"


class ClusterConfig(BaseModel):
    """Configuration for a Google Cloud Dataproc cluster.

    Provides sensible defaults for Open Targets platform workloads. All fields
    map directly to parameters accepted by the Airflow
    :class:`~airflow.providers.google.cloud.operators.dataproc.ClusterGenerator`,
    unless otherwise noted.

    The default implementation creates a cluster with 1 master node and 2 primary workers using
    n1-standard machine types and SSD boot disks, with a 2 hour idle deletion TTL. Adjust these defaults as needed for your workloads.
    """

    project_id: str = GCP_PROJECT_PLATFORM
    """Google Cloud project ID in which to create the cluster. Defaults to :data:`~orchestration.utils.common.GCP_PROJECT_PLATFORM`."""
    zone: str | None = GCP_ZONE
    """Google Cloud zone in which to create the cluster. Defaults to :data:`~orchestration.utils.common.GCP_ZONE`."""

    custom_image: str | None = None
    """Custom Dataproc image URI to use instead of a versioned public image."""
    custom_image_project_id: str | None = None
    """Google Cloud project ID that owns the custom image."""
    custom_image_family: str | None = None
    """Image family for the custom Dataproc image, used to resolve the latest image in the family."""
    image_version: str | None = "2.2"
    """Dataproc software version for the cluster. Defaults to ``"2.2"``."""

    autoscaling_policy: str | None = None
    """Autoscaling policy resource name. If only a short policy ID is supplied
    (i.e. no ``/`` characters), the full resource path is constructed automatically
    from :attr:`project_id` and the region derived from :attr:`zone`."""

    # Master node configuration
    num_masters: int = 1
    """Number of master nodes. Defaults to ``1``."""
    master_machine_type: str = "n1-highmem-16"
    """Compute Engine machine type for master nodes. Defaults to ``"n1-highmem-16"``."""
    master_disk_type: str = "pd-ssd"
    """Boot disk type for master nodes. Defaults to ``"pd-ssd"``."""
    master_disk_size: int = 512
    """Boot disk size in GB for master nodes. Defaults to ``512``."""
    master_accelerator_type: str | None = None
    """GPU accelerator type to attach to master nodes, or ``None`` for no GPU."""
    master_accelerator_count: int | None = None
    """Number of GPU accelerators to attach to each master node."""

    # Primary worker node configuration (not autoscaled)
    num_workers: int | None = 2
    """Number of primary worker nodes. Set to ``0`` for single-node mode. Defaults to ``2``."""
    min_num_workers: int | None = None
    """Minimum number of primary worker nodes required for the cluster to reach RUNNING state.
    If fewer VMs than this threshold are successfully created, the cluster is placed in ERROR
    state and the failed VMs are not deleted. If more than this threshold but fewer than
    :attr:`num_workers` VMs are created, the cluster is resized to the available count."""
    num_preemptible_workers: int = 0
    """Number of secondary (preemptible) worker nodes. Defaults to ``0``."""
    worker_machine_type: str = "n1-standard-4"
    """Compute Engine machine type for primary worker nodes. Defaults to ``"n1-standard-4"``."""
    worker_disk_type: str = "pd-ssd"
    """Boot disk type for primary worker nodes. Defaults to ``"pd-ssd"``."""
    worker_disk_size: int = 2048
    """Boot disk size in GB for primary worker nodes. Defaults to ``2048``."""
    worker_accelerator_type: str | None = None
    """GPU accelerator type to attach to primary worker nodes, or ``None`` for no GPU."""
    worker_accelerator_count: int | None = None
    """Number of GPU accelerators to attach to each primary worker node."""

    # Secondary worker node configuration (for autoscaling and preemptible workers)
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
    """Number of nodes in the dedicated driver node group. Defaults to ``0`` (no driver pool)."""
    driver_pool_id: str | None = None
    """Identifier for the driver node pool."""

    idle_delete_ttl: int | None = 7200
    """Seconds of inactivity after which the cluster is automatically deleted. Defaults to ``7200`` (2 hours)."""
    auto_delete_time: datetime | None = None
    """Absolute timestamp at which the cluster will be automatically deleted."""
    auto_delete_ttl: int | None = None
    """Seconds from cluster creation after which the cluster will be automatically deleted."""
    customer_managed_key: str | None = None
    """Cloud KMS key URI used for customer-managed disk encryption."""
    enable_component_gateway: bool | None = True
    """Whether to enable the Component Gateway, which exposes web UIs for installed optional components. Defaults to ``True``."""

    network_uri: str | None = None
    """Network URI for inter-node communication. Mutually exclusive with :attr:`subnetwork_uri`."""
    subnetwork_uri: str | None = None
    """Subnetwork URI for inter-node communication. Mutually exclusive with :attr:`network_uri`."""
    internal_ip_only: bool | None = None
    """If ``True``, all cluster instances will be assigned only internal IP addresses. Only valid when :attr:`subnetwork_uri` is set."""
    optional_components: list[str] | None = None
    """List of optional Dataproc components to install. See the `Dataproc component reference <https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#Component>`_."""
    preemptibility: str = PreemptibilityType.PREEMPTIBLE.value
    """Preemptibility model for secondary worker nodes. Defaults to :attr:`~airflow.providers.google.cloud.operators.dataproc.PreemptibilityType.PREEMPTIBLE`. See the `Dataproc RPC reference <https://cloud.google.com/dataproc/docs/reference/rpc/>`_."""

    tags: list[str] | None = None
    """Network tags applied to all cluster instances. Note: instance labels are set on the cluster creation operator, not here."""
    storage_bucket: str | None = None
    """Cloud Storage bucket used as the cluster staging bucket. If ``None``, Dataproc creates and manages one automatically."""
    metadata: dict | None = None
    """Compute Engine instance metadata key-value pairs applied to all cluster instances."""
    properties: dict | None = None
    """Dataproc and Hadoop configuration properties applied to cluster config files."""

    init_actions_uris: list[str] | None = None
    """List of Cloud Storage URIs pointing to initialisation scripts run on every node after cluster creation."""
    init_action_timeout: str = "10m"
    """Timeout for each initialisation action. Defaults to ``"10m"`` (10 minutes)."""

    service_account: str | None = GCP_SERVICE_ACCOUNT
    """Service account email attached to all cluster VMs. Defaults to :data:`~orchestration.utils.common.GCP_SERVICE_ACCOUNT`."""
    service_account_scopes: list[str] | None = None
    """OAuth scopes granted to the cluster service account."""

    def model_post_init(self, _: Any) -> None:
        if isinstance(self.autoscaling_policy, str) and "/" not in self.autoscaling_policy:
            zone = self.zone or GCP_ZONE
            region = zone.rsplit("-", 1)[0]
            ap = f"projects/{self.project_id}/regions/{region}/autoscalingPolicies/{self.autoscaling_policy}"
            self.autoscaling_policy = ap

    @staticmethod
    def _update_c4_machine_disk_config(disk_config: DiskConfig) -> DiskConfig:
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

    def create_cluster(self) -> Cluster:
        """Build a Dataproc :class:`~google.cloud.dataproc_v1.Cluster` object from this configuration.

        Delegates to the Airflow
        :class:`~airflow.providers.google.cloud.operators.dataproc.ClusterGenerator`
        and applies additional disk configuration overrides for ``c4-`` machine
        types, which require ``hyperdisk-balanced`` boot disks.

        Checks for secondary worker machine type, disk and size and overrides the relevant ClusterConfig
        fields if they are set. This is required as otherwise the secondary workers will
        inherit the same machine type and disk config as the primary workers, which may not be desirable when using autoscaling or preemptible workers.
        Especially when using efm mode.

        Returns:
            Cluster: A Dataproc cluster configuration object ready for submission.
        """
        exclude_fields = {"secondary_worker_disk_type", "secondary_worker_disk_size", "secondary_worker_machine_type"}
        config = ClusterGenerator(**self.model_dump(exclude=exclude_fields)).make()

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


class ClusterDefinition(InfrastructureDefinition[ClusterConfig]):
    """Concrete infrastructure definition for a Google Cloud Dataproc cluster.

    Pairs the ``DATAPROC_CLUSTER`` infrastructure identifier with a validated
    :class:`ClusterConfig`, making it suitable for registration in a
    :class:`ClusterRegistry` and reference from a pipeline step.
    """

    infrastructure: str = INFRASTRUCTURE
    """Infrastructure type identifier. Always set to ``"DATAPROC_CLUSTER"``."""
    config: ClusterConfig
    """Full configuration for the Dataproc cluster."""


class SparkJobPropertyMapping(BaseModel):
    """Configuration for a single Spark property to be applied to the cluster."""

    properties: dict[str, str]
    """Mapping of Spark property keys to values. Keys must start with "spark."."""

    @field_validator("properties", mode="after")
    @classmethod
    def validate_keys(cls, name: str) -> str:
        """Validate that the property name starts with "spark."."""
        if not name.startswith("spark."):
            raise ValueError(f"Invalid Spark Job property name '{name}'.Spark property names must start with 'spark.'.")
        return name


class ClusterRegistry(InfrastructureRegistry[ClusterConfig]):
    """Registry of named :class:`ClusterConfig` configurations.

    Pipeline steps can reference entries in this registry by name, enabling
    reuse of common cluster configurations across multiple steps.
    """

    step_job_properties: dict[str, SparkJobPropertyMapping] | None = None
    """Optional mapping of step short names to Spark job property mappings.
        If provided, these properties will be applied to the cluster configuration when a step with the corresponding short name is executed.
        This allows users to specify step-specific Spark properties that are not directly exposed by the ClusterConfig model."""
