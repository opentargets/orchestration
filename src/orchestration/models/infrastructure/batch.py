"""Models for batch processing infrastructure specifications."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from orchestration.utils.common import GCP_PROJECT_PLATFORM

INFRASTRUCTURE = "GOOGLE_BATCH_JOB"


class BatchJob(BaseModel):
    """Google Batch job configuration class."""

    infrastructure: str = INFRASTRUCTURE
    """Infrastructure type for the batch job, set to "GOOGLE_BATCH_JOB"."""
    project_id: str = GCP_PROJECT_PLATFORM
    """Google cloud project ID in which to run the batch job. Default is GCP_PROJECT_PLATFORM."""
    index_specs: IndexSpecs
    """Specification for the BatchIndex see: :mod:`orchestration.operators.batch` for more details."""
    job_specs: JobSpecs
    """Specification for the BatchJob see: :mod:`orchestration.operators.batch` for more details."""


class JobSpecs(BaseModel):
    """Specification for the BatchJob."""

    entrypoint: str = "/usr/bin/bash"
    """Container entrypoint to use when submitting the job. Default is "/usr/bin/bash" to allow running arbitrary commands in the container."""
    image: str
    """Container image to use for the job. Should be provided either defined in GAR or public container registry. """
    resource_specs: ResourceSpecs
    """Resource specification including cpu, memory and boot disk that are defined for each task in the job."""
    task_specs: TaskSpecs
    """Task specification including retry policy and max duration for each task in the job."""
    policy_specs: PolicySpecs
    """Policy specification including machine type for each task in the job."""


class TaskSpecs(BaseModel):
    """Specification for the BatchJob task."""

    max_retry_count: int = 0
    """Maximum number of retries for each task in the job. Default is 0, meaning no retries."""
    max_run_duration: str = "1h"
    """Maximum run duration for each task in the job. Default is "1h" (1 hour). Should be provided in the format accepted by Google Batch API, e.g. "30m" for 30 minutes, "2h" for 2 hours, etc."""


class PolicySpecs(BaseModel):
    """Specification for the BatchJob policy."""

    machine_type: str = "n1-standard-2"
    """Machine type for each task in the job. Default is "n1-standard-2"."""


class ResourceSpecs(BaseModel):
    """Specification for the BatchJob resources."""

    cpu_milli: int
    """Amount of CPU in milliseconds per CPU-second to use for each task in the job. For example, 2000 means 2 whole CPUs, 500 means half a CPU, etc."""
    memory_mib: int
    """Amount of memory in MiB to use for each task in the job."""
    boot_disk_mib: int
    """Amount of boot disk in MiB to use for each task in the job."""


class IndexSpecs(BaseModel):
    """Specification for the BatchIndex, which is used for dynamic job generation based on a manifest of inputs."""

    manifest_generator_label: str
    """Label to identify the manifest generator implementation."""
    manifest_generator_specs: dict[str, Any] | None = None
    """Keyword arguments to be passed to the manifest generator implementation."""
    max_task_count: int = 100
    """Maximum number of tasks to generate for the batch job. Default is 100. If set to 0, then the number of tasks will be determined by the number of rows in the generated manifest."""


__all__ = [
    "INFRASTRUCTURE",
    "BatchJob",
    "IndexSpecs",
    "JobSpecs",
    "PolicySpecs",
    "ResourceSpecs",
    "TaskSpecs",
]
