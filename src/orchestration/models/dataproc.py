"""Dataproc config models."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, model_validator

from orchestration.operators.dataproc import ClusterConfig


class DataprocConfig(BaseModel):
    """Pydantic model for Dataproc clusters and job properties."""

    clusters: dict[str, ClusterConfig]
    """Clusters available in the configuration."""
    step_job_properties: dict[str, dict[str, str]] | None = None
    """Job properties for specific steps. Keys are step names, values are job properties."""

    @model_validator(mode="after")
    def validate_model(self) -> Self:
        """Validate the model after initialization.

        Ensure that:
        * All clusters have unique names.
        * All step_job_properties refer to one of defined tools that use cluster.
        """
        for p in self.step_job_properties or {}:
            if p not in ["etl", "gentropy"]:
                raise ValueError(f"Step '{p}' has job properties but no corresponding cluster")
        return self
