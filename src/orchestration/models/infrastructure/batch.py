"""Models for Google Batch job infrastructure specifications.

This module defines Pydantic models used to configure and validate Google Batch
job infrastructure. These models cover the full configuration surface for
submitting batch jobs, including resource allocation, machine policy, task
behaviour, and dynamic input indexing via manifest generators.

Typical usage involves constructing a :class:`BatchDefinition` (or registering
one in a :class:`BatchJobRegistry`) and referencing it from a pipeline step.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from orchestration.models.infrastructure.abc import InfrastructureDefinition, InfrastructureRegistry
from orchestration.utils.common import GCP_PROJECT_PLATFORM

INFRASTRUCTURE = "GOOGLE_BATCH_JOB"


class BatchConfig(BaseModel):
    """Top-level configuration for a Google Batch job.

    Groups together the project targeting, input indexing strategy, and job
    execution specification required to submit a batch job.
    """

    project_id: str = GCP_PROJECT_PLATFORM
    """Google Cloud project ID in which to run the batch job. Defaults to :data:`~orchestration.utils.common.GCP_PROJECT_PLATFORM`."""
    index_specs: IndexSpecs
    """Input indexing specification used for dynamic, manifest-driven task generation. See :class:`IndexSpecs`."""
    job_specs: JobSpecs
    """Execution specification for the batch job, including image, resources, and task policy. See :class:`JobSpecs`."""


class IndexSpecs(BaseModel):
    """Specification for dynamic task indexing based on a generated manifest.

    At runtime, a manifest generator identified by :attr:`manifest_generator_label`
    is invoked to produce a list of inputs. Each row in the manifest becomes an
    individual task in the batch job, up to :attr:`max_task_count`.
    """

    manifest_generator_label: str
    """Label used to look up the manifest generator implementation at runtime."""
    manifest_generator_specs: dict[str, Any] | None = None
    """Optional keyword arguments forwarded to the manifest generator implementation."""
    max_task_count: int = 100
    """Upper bound on the number of tasks created from the manifest. Defaults to ``100``. Set to ``0`` to allow an unlimited number of tasks (bounded only by the manifest size)."""


class JobSpecs(BaseModel):
    """Execution specification for a Google Batch job.

    Defines the container image and entrypoint together with the resource,
    policy, and task-level specifications that apply to every task in the job.
    """

    entrypoint: str = "/usr/bin/bash"
    """Container entrypoint used when submitting the job. Defaults to ``/usr/bin/bash``, allowing arbitrary shell commands to be executed inside the container."""
    image: str
    """Fully-qualified container image URI. The image must be accessible from either Google Artifact Registry (GAR) or a public container registry."""
    resource_specs: ResourceSpecs
    """Per-task resource allocation (CPU, memory, and boot disk). See :class:`ResourceSpecs`."""
    policy_specs: PolicySpecs
    """Machine-level policy for each task, including the Compute Engine machine type. See :class:`PolicySpecs`."""
    task_specs: TaskSpecs
    """Task-level execution policy covering retry behaviour and maximum run duration. See :class:`TaskSpecs`."""


class ResourceSpecs(BaseModel):
    """Per-task resource allocation for a Google Batch job.

    All values are applied uniformly to every task in the job.
    """

    cpu_milli: int
    """CPU allocation in milli-CPUs (1/1000 of a CPU) per task. For example, ``2000`` corresponds to 2 full CPUs and ``500`` corresponds to half a CPU."""
    memory_mib: int
    """Memory allocation in mebibytes (MiB) per task."""
    boot_disk_mib: int
    """Boot disk size in mebibytes (MiB) per task."""


class PolicySpecs(BaseModel):
    """Machine-level provisioning policy for a Google Batch job."""

    machine_type: str = "n1-standard-2"
    """Compute Engine machine type used for each task. Defaults to ``n1-standard-2``."""


class TaskSpecs(BaseModel):
    """Task-level execution policy for a Google Batch job.

    Controls retry behaviour and the maximum wall-clock time allowed for a
    single task.
    """

    max_retry_count: int = 0
    """Maximum number of times a failed task will be retried. Defaults to ``0`` (no retries)."""
    max_run_duration: str = "1h"
    """Maximum wall-clock duration allowed for a single task. Defaults to ``"1h"``. Must be expressed in the format accepted by the Google Batch API (e.g. ``"30m"``, ``"2h"``, ``"90s"``)."""


class BatchDefinition(InfrastructureDefinition[BatchConfig]):
    """Concrete infrastructure definition for a Google Batch job.

    Pairs the ``GOOGLE_BATCH_JOB`` infrastructure identifier with a validated
    :class:`BatchConfig`, making it suitable for registration in a pipeline.
    """

    infrastructure: str = INFRASTRUCTURE
    """Infrastructure type identifier. Always set to ``"GOOGLE_BATCH_JOB"``."""
    config: BatchConfig
    """Full configuration for the batch job infrastructure."""


class BatchJobRegistry(InfrastructureRegistry[BatchConfig]):
    """Registry of named :class:`BatchConfig` configurations.

    Pipeline steps can reference entries in this registry by name, enabling
    reuse of common batch job configurations across multiple steps.
    """
