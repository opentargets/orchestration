"""Staging pipeline step class definition."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel

from orchestration.models.infrastructure import InfrastructurePointer
from orchestration.models.infrastructure.batch import BatchJob
from orchestration.models.infrastructure.dataproc import ClusterDefinition


class StepConfig(BaseModel):
    """Configuration for a single step in the staging pipeline."""

    id: str
    """Unique identifier for the step."""
    infrastructure: InfrastructurePointer
    """Infrastructure pointer for the step. This informs the pipeline which infrastructure specifications to use for executing this step."""
    command: str
    """Command to execute for this step."""
    prerequisites: list[str] | None = None
    """List of step IDs that are prerequisites for this step."""


class EnvSpec(BaseModel):
    """Model representing the single environment specification ."""

    name: str
    """Name of the environment, e.g. "Prod", "Test", etc."""
    vars: dict[str, str]
    """Dictionary of environment variable names and their corresponding values."""


class Environments(BaseModel):
    """Model representing all environments.

    This class implements the registry of environment specs with `get_vars` method to retrieve the templating context for a specific environment.
    """

    environments: list[EnvSpec]
    """List of environment specifications."""

    def get_vars(self, env_name: Env) -> dict[str, str]:
        """Get the environment variable values for the specified environment.

        Args:
            env_name: The environment to get the variable values for.

        Returns:
            A dictionary of environment variable names and their corresponding values for the specified environment.
        """
        for env_spec in self.environments:
            if env_spec.name == env_name.env:
                return env_spec.vars
        raise ValueError(f"Environment '{env_name.env}' not found in configuration.")


class StepRegistry(BaseModel):
    """Registry for step configurations, which can be referenced by the DAG to construct the pipeline."""

    steps: dict[str, StepConfig]
    """Mapping of step IDs to step configurations to execute in the pipeline."""

    def get(self, step_id: str) -> StepConfig:
        """Get a step configuration by step ID.

        Args:
            step_id: The ID of the step configuration to retrieve.

        Returns:
            StepConfig: The step configuration with the specified step ID.

        Raises:
            ValueError: If no step configuration with the specified step ID is found.
        """
        if step_id not in self.steps:
            raise ValueError(f"Step configuration '{step_id}' not found in registry.")
        return self.steps[step_id]

    def __iter__(self):
        """Iterate over the step configurations in the registry."""
        return iter(self.steps.values())


class ClusterRegistry(BaseModel):
    """Registry for cluster definitions.

    This class is used to store and retrieve cluster definitions by name.
    """

    clusters: dict[str, ClusterDefinition]
    """Dict mapping cluster names to cluster definitions."""

    def ensure(self, required_cluster_names: set[str]) -> Self:
        """Ensure that all required clusters are present in the registry.

        Args:
            required_cluster_names: A set of cluster names that are required by the pipeline steps.

        Raises:
            ValueError: If any required cluster name is not found in the registry.
        """
        missing_clusters = required_cluster_names - self.clusters.keys()
        if missing_clusters:
            raise ValueError(f"Missing required cluster definitions in registry: {missing_clusters}")
        return self

    def get(self, cluster_name: str) -> ClusterDefinition:
        """Get a cluster definition by name.

        Args:
            cluster_name: The name of the cluster definition to retrieve.

        Returns:
            ClusterDefinition: The cluster definition with the specified name.

        Raises:
            ValueError: If no cluster definition with the specified name is found.
        """
        if cluster_name not in self.clusters:
            raise ValueError(f"Cluster definition '{cluster_name}' not found in registry.")
        return self.clusters[cluster_name]


class BatchJobRegistry(BaseModel):
    """Registry for BatchJob configurations, which can be referenced by steps in the staging pipeline."""

    jobs: dict[str, BatchJob]
    """Dictionary mapping job IDs to BatchJob configurations."""

    def ensure(self, required_job_ids: set[str]) -> Self:
        """Ensure that all required BatchJob configurations are present in the registry.

        Args:
            required_job_ids: A set of job IDs that are required by the pipeline steps.

        Raises:
            ValueError: If any required job ID is not found in the registry.
        """
        missing_jobs = required_job_ids - self.jobs.keys()
        if missing_jobs:
            raise ValueError(f"Missing required BatchJob configurations in registry: {missing_jobs}")
        return self

    def get(self, job_id: str) -> BatchJob:
        """Get a BatchJob configuration by job ID.

        Args:
            job_id: The ID of the BatchJob configuration to retrieve.

        Returns:
            BatchJob: The BatchJob configuration with the specified job ID.

        Raises:
            ValueError: If no BatchJob configuration with the specified job ID is found.
        """
        if job_id not in self.jobs:
            raise ValueError(f"BatchJob configuration '{job_id}' not found in registry.")
        return self.jobs[job_id]


__all__ = [
    "Env",
    "EnvSpec",
    "Environments",
    "StepConfig",
]
