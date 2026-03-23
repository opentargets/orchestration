"""Infrastructure models for orchestration.

The infrastructure models comprise the specification for
* Google Batch jobs with dynamic job generation based on a manifest of inputs,
* Dataproc Cluster definitions

"""

from typing import Literal

from pydantic import BaseModel

from orchestration.models.infrastructure.batch import INFRASTRUCTURE as BATCH_INFRASTRUCTURE
from orchestration.models.infrastructure.dataproc import INFRASTRUCTURE as DATAPROC_INFRASTRUCTURE


class InfrastructurePointer(BaseModel):
    """Pointer to infrastructure specifications for a pipeline step.

    This class is used in the step configuration to point to the infrastructure specifications
    for the step, which can be either a Batch job or a Dataproc cluster.
    """

    type: Literal["GOOGLE_BATCH_JOB", "DATAPROC_CLUSTER"]
    """Type of the infrastructure, e.g. "GOOGLE_BATCH_JOB" for Batch jobs, "DATAPROC_CLUSTER" for Dataproc clusters, etc."""
    pointer: str
    """Name pointing to specific infrastructure specifications. Used to look up the registry for the proper implementation."""


__all__ = [
    "BATCH_INFRASTRUCTURE",
    "DATAPROC_INFRASTRUCTURE",
    "InfrastructurePointer",
]
