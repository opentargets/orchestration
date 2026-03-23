"""Models for Dataproc infrastructure specifications."""

from datetime import datetime

from airflow.providers.google.cloud.operators.dataproc import InstanceFlexibilityPolicy, PreemptibilityType
from google.cloud.dataproc_v1 import Cluster
from pydantic import BaseModel

from orchestration.models.secret import Secret
from orchestration.utils import resource_name
from orchestration.utils.common import GCP_PROJECT_PLATFORM, GCP_SERVICE_ACCOUNT, GCP_ZONE
from orchestration.utils.dataproc import ClusterGenerator

INFRASTRUCTURE = "DATAPROC_CLUSTER"


class ClusterConfig(BaseModel):
    """Dataproc cluster configuration class.

    Includes defaults tailored to our cluster needs.
    """

    infrastructure: str = INFRASTRUCTURE
    """Infrastructure type for the cluster, set to "DATAPROC_CLUSTER"."""
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

    def model_post_init(self) -> None:
        if isinstance(self.autoscaling_policy, str) and "/" not in self.autoscaling_policy:
            zone = self.zone or GCP_ZONE
            region = zone.rsplit("-", 1)[0]
            ap = f"projects/{self.project_id}/regions/{region}/autoscalingPolicies/{self.autoscaling_policy}"
            self.autoscaling_policy = ap

    def create_cluster(self) -> Cluster:
        """Create a Dataproc cluster from the configuration.

        Returns:
            Cluster: The Dataproc cluster.
        """
        config = ClusterGenerator(**self.model_dump()).make()
        # Ensure that the c4- machine types have the right disk config
        # TODO: Refactor once we are sure we need the c4- machine types
        if self.worker_machine_type.startswith("c4-"):
            config["worker_config"]["disk_config"]["boot_disk_type"] = "hyperdisk-balanced"
            config["worker_config"]["disk_config"]["boot_disk_provisioned_iops"] = 6_000
            # Default is 140+ 1.5 x 500GiB
            config["worker_config"]["disk_config"]["boot_disk_provisioned_throughput"] = 500
        if self.master_machine_type.startswith("c4-"):
            config["master_config"]["disk_config"]["boot_disk_type"] = "hyperdisk-balanced"
            config["master_config"]["disk_config"]["boot_disk_provisioned_iops"] = 6_000
            config["master_config"]["disk_config"]["boot_disk_provisioned_throughput"] = 500
        return config


class ClusterDefinition(BaseModel):
    """Cluster definition.

    This class is used to define the cluster configuration for a step in the
    pipeline. It contains the cluster type and configuration.
    """

    cluster_type: str
    """The type of the cluster."""
    config: ClusterConfig
    """The configuration dict for the cluster. See
        `src.orchestration.utils.dataproc.ClusterConfig`."""
    secrets: list[Secret] | None = None

    @property
    def cluster_name(self) -> str:
        """Returns the resource name for this cluster definition."""
        return resource_name(self.cluster_type)

    @property
    def cluster_config(self) -> ClusterConfig:
        """Returns the ClusterConfig object for this cluster definition."""
        return self.config


__all__ = [
    "INFRASTRUCTURE",
    "ClusterConfig",
    "ClusterDefinition",
]
