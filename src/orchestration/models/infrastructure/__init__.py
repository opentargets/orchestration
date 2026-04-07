"""Infrastructure models for orchestration.

This package provides Pydantic models for defining, registering, and referencing
Google Cloud infrastructure used by pipeline steps. Two infrastructure backends
are supported:

- **Google Batch** - serverless batch job execution with dynamic, manifest-driven
  task generation. See :mod:`~orchestration.models.infrastructure.batch`.
- **Dataproc** - managed Spark/Hadoop cluster execution. See
  :mod:`~orchestration.models.infrastructure.dataproc`.

Common abstractions (registry, definition, pointer) are provided by
:mod:`~orchestration.models.infrastructure.abc` and shared across both backends.
"""

from enum import StrEnum

from pydantic import BaseModel

from orchestration.models.infrastructure import batch, dataproc
from orchestration.models.infrastructure.batch import (
    BatchConfig,
    BatchDefinition,
    BatchJobRegistry,
    IndexSpecs,
)
from orchestration.models.infrastructure.dataproc import (
    ClusterConfig,
    ClusterDefinition,
    ClusterRegistry,
)


class UnknownInfrastructureTypeError(Exception):
    """Custom exception for unknown infrastructure types in the staging pipeline configuration."""

    def __init__(self, infrastructure_type: str):
        super().__init__(f"Unknown infrastructure type: {infrastructure_type}")
        self.infrastructure_type = infrastructure_type


class InfrastructureType(StrEnum):
    DATAPROC = dataproc.INFRASTRUCTURE
    BATCH = batch.INFRASTRUCTURE


class InfrastructurePointer(BaseModel):
    """Lightweight reference from a pipeline step to an infrastructure definition.

    At runtime, the :attr:`type` field is used to select the appropriate
    registry, and :attr:`pointer` is used to look up the specific definition
    within that registry.
    """

    type: InfrastructureType
    """Infrastructure type identifier that determines which registry to search (e.g. ``"GOOGLE_BATCH_JOB"``, ``"DATAPROC_CLUSTER"``)."""
    pointer: str
    """Name of the infrastructure definition to retrieve from the registry."""


__all__ = [
    # batch
    "BatchConfig",
    "BatchDefinition",
    "BatchJobRegistry",
    # dataproc
    "ClusterConfig",
    "ClusterDefinition",
    "ClusterRegistry",
    "IndexSpecs",
    # abc
    "InfrastructurePointer",
    "InfrastructureType",
]
