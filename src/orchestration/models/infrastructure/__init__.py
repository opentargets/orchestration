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

from orchestration.models.infrastructure.abc import InfrastructurePointer
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
]
