from __future__ import annotations

from logging import Logger
from pathlib import Path
from typing import Protocol, overload

from orchestration.models.environment import Env, Environments
from orchestration.models.infrastructure import (
    BatchJobRegistry,
    ClusterRegistry,
    InfrastructureType,
    UnknownInfrastructureTypeError,
)
from orchestration.models.step.abc import StepConfigRegistry, StepDefinition


class PipelineConfig(Protocol):
    """Protocol representing the configuration for the staging pipeline, which can be used to construct the DAG."""

    logger: Logger
    """Logger for the staging pipeline configuration."""
    env: Env
    """Environment to run the pipeline in, e.g. "Prod", "Test", etc. The environment is used to derive the templating context to pre-fill the configuration."""
    environments: Environments
    """List of environment specifications for the staging pipeline, including environment variable values that can be used in the pipeline's tasks."""
    run_name: str
    """Used for labelling resources."""
    clusters: ClusterRegistry
    """Cluster registry for the pipeline."""
    batch_jobs: BatchJobRegistry
    """Batch job registry for the pipeline."""
    steps: StepConfigRegistry
    """Step registry for the pipeline."""

    @overload
    def __init__(self) -> None: ...

    @overload
    def __init__(self, config_path: Path) -> None: ...

    @overload
    def __init__(self, config_path: Path, clusters_config_path: Path, batch_jobs_config_path: Path) -> None: ...

    def __init__(self, *args, **kwargs) -> None: ...

    def step_definition(self, step_name: str) -> StepDefinition:
        """Return the definition of a step.

        This method returns the step definition, which includes the step name,
        command, infrastructure pointer, and any other metadata required to execute the step.
        """
        config = self.steps.get(step_name)
        match config.infrastructure.type:
            case InfrastructureType.DATAPROC:
                infrastructure_definition = self.clusters.get(config.infrastructure.pointer)
            case InfrastructureType.BATCH:
                infrastructure_definition = self.batch_jobs.get(config.infrastructure.pointer)
            case _:
                raise UnknownInfrastructureTypeError(config.infrastructure.type)

        return StepDefinition(
            name=step_name,
            command=config.command,
            infrastructure=config.infrastructure,
            prerequisites=config.prerequisites,
            definition=infrastructure_definition,
        )
