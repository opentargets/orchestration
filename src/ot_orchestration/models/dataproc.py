"""Dataproc configuration."""

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator
from typing_extensions import Self

from ot_orchestration.common import (
    DATAPROC_BASE_PROPERTIES,
    DATAPROC_EFM_MODE_PROPERTIES,
    GCP_AUTOSCALING_POLICY,
    GCP_DATAPROC_IMAGE,
    GCP_EFM_AUTOSCALING_POLICY,
    GCP_PROJECT_GENETICS,
    GCP_REGION,
    GCP_ZONE,
    GENTROPY_CLUSTER_INIT_SCRIPT,
)
from ot_orchestration.models.labels import LabelModel
from ot_orchestration.utils.path import IOManager


class GentropyMetadata(BaseModel):
    ref: str


class DataprocClusterConfigModel(BaseModel):
    """Dataproc cluster specification.

    Most of the options can be passed directly to the `airflow.providers.google.cloud.operators.dataproc.ClusterGenerator`

    Additional options that are utilized only by the `DataprocCreateClusterConfigGenerateOperator`
    * `allow_efm` (bool): Defaults to False. Allows for the `Dataproc Enhanced Flexibility Mode <https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/enhanced-flexibility-mode>`__
    """

    # Cluster global config
    cluster_name: str
    region: str = GCP_REGION
    project_id: str = GCP_PROJECT_GENETICS
    zone: str = GCP_ZONE
    autoscaling_policy: str = GCP_AUTOSCALING_POLICY
    properties: dict[str, str] = DATAPROC_BASE_PROPERTIES
    metadata: GentropyMetadata | None
    init_actions_uris: list[str] = [GENTROPY_CLUSTER_INIT_SCRIPT]
    idle_delete_ttl: int = 30 * 30  # In seconds
    image_version: Literal["2.1", "2.2"] = GCP_DATAPROC_IMAGE
    labels: LabelModel
    num_local_ssds: int = 1

    # Master config
    num_masters: int = 1
    master_machine_type: str = "n1-standard-4"
    master_disk_type: Literal["pd-standard", "pd-ssd"] = "pd-standard"
    master_disk_size: int = 500

    # Worker config
    num_workers: int = 0
    worker_machine_type: str = "n1-highmem-16"
    worker_dist_type: Literal["pd-standard", "pd-ssd"] = "pd-ssd"
    worker_disk_size: int = 1024 * 2
    num_preemptible_workers: int = 0
    preempibility: Literal["PREEMPTIBLE", "SPOT"] = "PREEMPTIBLE"

    # access config
    service_account: str | None = None
    enable_component_gateway: bool = True
    internal_ip_only: bool = True

    # Custom config
    allow_efm: bool = False

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def ensure_efm_mode_requirements(self) -> Self:
        """Ensure the configuration aligns with the EFM mode when mode is requested."""
        if self.allow_efm:
            self.properties = {**self.properties, **DATAPROC_EFM_MODE_PROPERTIES}
            if self.num_masters != 3:
                raise ValueError("`allow_efm=True` requires 3 master nodes")
            if self.num_workers < 10:
                raise ValueError("`allow_efm=True` requires minimum 10 primary workers to store tha cache.")
            if self.autoscaling_policy != GCP_EFM_AUTOSCALING_POLICY:
                raise ValueError(f"`allow_efm=True` requires {GCP_EFM_AUTOSCALING_POLICY} policy")
        return self

    @model_validator(mode="after")
    def ensure_init_actions_uri_is_proper_uri(self) -> Self:
        """Ensure init actions point to google cloud storage uri(s)."""
        for uri in self.init_actions_uris:
            parsed_uri = IOManager().resolve(uri)
            if not parsed_uri.is_gcs_path:
                raise ValueError("`init_actions_uri` must point to google cloud storage paths.")
        return self

    @model_validator(mode="after")
    def ensure_external_ip(self) -> Self:
        """Ensure external ip for the cluster is enabled.

        `internal_ip_only=False` is required for the dataproc images with version 2.2 or above, to access the internet via NAT gateway.
        See `internal ip only mode <https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/network#:~:text=Note%3A%20Internal%20IP%20only%20and%20Private%20Google%20Access%20are%20enabled%20by%20default%20when%20creating%20Dataproc%20clusters%20with%202.2%20and%20later%20image%20versions.>`__
        """
        pattern = re.compile(r"^(?P<major>\d+).(?P<minor>\d+).*$")
        _match = pattern.match(self.image_version)
        if not _match:
            raise ValueError("Provide correct dataproc image version.")
        major = _match.group("major")
        minor = _match.group("minor")
        version = int(f"{major}.{minor}")
        if version >= 2.2:
            if not self.internal_ip_only:
                raise ValueError(
                    "When using dataproc image > 2.2, external internet access requires `internal_ip_only=False`"
                )
        return self

    @model_validator(mode="after")
    def ensure_autoscaling_policy_format(self) -> Self:
        """Ensure the format of Dataproc autoscaling policy."""
        self.policy_name = f"projects/{self.project_id}/regions/{self.region}/autoscalingPolicies/{self.policy_name}"
        return self


class DataprocClusterCreateModel(BaseModel):
    cluster_name: str
    cluster_config: DataprocClusterConfigModel


class DataprocClusterDeleteModel(BaseModel):
    cluster_name: str


class DataprocSubmitPysparkJobModel(BaseModel):
    cluster_name: str


class DataprocSubmitSparkJobModel(BaseModel):
    cluster_name: str
