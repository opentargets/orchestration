"""Staging pipeline models."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, model_validator


class StagingPipelineStepParams(BaseModel):
    """Pydantic model for a pipeline step."""

    depends_on: list[str] | None = None
    """List of step names that this step depends on."""
    cluster: str | None = None
    """Cluster name to use for the job."""
    vm: str | None = None
    """VM name to use for the job"""


class Environment(BaseModel):
    """Pydantic model for environment specifications."""

    name: Literal["dev", "prod"]
    """Name of the environment."""
    sentinels: dict[str, str]
    """Dictionary of sentinels for the environment."""


class StagingPipelineConfig(BaseModel):
    """Pydantic model for Gentropy pipeline configuration.

    This model is used to validate the configuration read from a YAML file.

    All steps in the pipeline must have unique names and valid names _(starting with
    "gentropy" or "gentroutils")_. Dependencies between steps must refer to valid
    step names.
    """

    environments: list[Environment]
    """List of environments available in the configuration."""
    env: Literal["dev", "prod"]
    """Current environment to use."""
    staging_bucket: str
    """Staging bucket to use for intermediate files."""
    steps: dict[str, StagingPipelineStepParams]
    """List of steps to execute in the pipeline."""

    @model_validator(mode="after")
    def validate_model(self) -> Self:
        """Validate the model after initialization.

        This method checks:
        * That the staging_bucket is a valid GCS path.
        * That all dependencies in steps are valid step names.
        * That all steps have unique and valid names.
        """
        if len(self.steps) != len(set(self.steps.keys())):
            raise ValueError("All steps must have unique names")

        for step_name, step in self.steps.items():
            if step_name.split("_")[0] not in ["gentropy", "gentroutils"]:
                raise ValueError(f"Step name '{step_name}' is not valid. Must start with 'gentropy' or 'gentroutils'")
            if step.depends_on:
                for dependency in step.depends_on:
                    if dependency not in self.steps:
                        raise ValueError(f"Step '{step_name}' has unknown dependency '{dependency}'")
        return self

    def get_sentinels_for_env(self) -> dict[str, str]:
        """Get the sentinels for the current environment."""
        for environment in self.environments:
            if environment.name == self.env:
                return environment.sentinels
        raise ValueError(f"Environment {self.env} not found in configuration")
