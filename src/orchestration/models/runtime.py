"""Runtime DAG task configuration."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, model_validator

from orchestration.models import StepName
from orchestration.operators.dataproc import ClusterConfig


class VMConfig(BaseModel):
    """Configuration for a VM instance."""

    image: str
    """Container image to run on the VM."""

    disk_size_gb: int
    """Size of the boot disk in GB."""

    machine_type: str
    """Machine type to use for the VM."""


class ClusterResource(BaseModel):
    """Reference to a Dataproc cluster."""

    name: str
    """Name of the cluster."""

    config: ClusterConfig
    """Configuration of the cluster."""

    labels: dict[str, str]
    """Labels to apply to the cluster."""


class VMResource(BaseModel):
    """Reference to a VM resource."""

    name: str
    """Name of the VM."""

    config: VMConfig
    """Configuration of the VM."""

    labels: dict[str, str]
    """Labels to apply to the VM."""


class RuntimeDagTaskConfig(BaseModel):
    """Runtime configuration for a DAG task."""

    step_name: StepName
    """Name of the step in the pipeline."""

    depends_on: list[RuntimeDagTaskConfig]
    """List of task names that this task depends on."""

    tool: Literal["gentroutils", "gentropy"]
    """Tool used in the task, e.g., 'gentroutils' or 'gentropy'."""

    cluster: ClusterResource | None
    """Dataproc cluster to run the task on, if applicable."""

    vm: VMResource | None
    """VM to run the task on, if applicable."""

    @model_validator(mode="after")
    def check_resource(self) -> RuntimeDagTaskConfig:
        if self.tool == "gentropy" and self.cluster is None:
            raise ValueError(f"Gentropy step {self.step_name} must have a cluster defined.")
        if self.tool == "gentroutils" and self.vm is None:
            raise ValueError(f"Gentroutils step {self.step_name} must have a VM defined.")
        return self
