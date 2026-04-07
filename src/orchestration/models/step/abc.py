from __future__ import annotations

import re
from collections.abc import Iterator
from enum import StrEnum

from pydantic import BaseModel, ValidationError, field_validator

from orchestration.models.infrastructure import InfrastructurePointer
from orchestration.models.infrastructure.abc import InfrastructureDefinition


class PipelineStage(StrEnum):
    """Enum representing different stages in the pipeline."""

    PIS = "pis"
    PTS = "pts"
    ETL = "etl"
    GENTROPY = "gentropy"

    @classmethod
    def from_step_name(
        cls,
        step_name: str,
    ) -> PipelineStage:
        """Returns the PipelineStage corresponding to the given step name."""
        step_prefix = step_name.split("_")[0].lower()
        return PipelineStage(step_prefix)


class StepConfig(BaseModel):
    """Configuration for a single step in the staging pipeline."""

    name: str
    """Unique identifier for the step."""
    infrastructure: InfrastructurePointer
    """Infrastructure pointer for the step. This informs the pipeline which infrastructure specifications to use for executing this step."""
    command: str
    """Command to execute for this step."""
    prerequisites: list[str] | None = None
    """List of step IDs that are prerequisites for this step."""

    @field_validator("name", mode="after")
    @classmethod
    def validate_name(cls, name: str) -> str:
        """Validate that the step name starts with a valid stage prefix."""
        stage_pattern = "|".join(re.escape(stage.value.lower()) for stage in PipelineStage)
        step_pattern = rf"^({stage_pattern})_(\d+)$"
        if not re.match(step_pattern, name, re.IGNORECASE):
            raise ValidationError(
                f"Invalid step name '{name}'. Step name must start with a valid stage prefix ({stage_pattern}) followed by an underscore and a unique identifier (e.g. `pis_study`)."
            )
        return name

    @property
    def short_name(self) -> str:
        """The short name of the step, without stage prefix."""
        return self.name.split("_", 1)[1].lower()

    @property
    def stage(self) -> PipelineStage:
        """The stage of the pipeline this step belongs to."""
        return PipelineStage.from_step_name(self.name)


class StepDefinition(StepConfig):
    """Definition of a step in the staging pipeline, including its configuration and any additional metadata required for execution."""

    definition: InfrastructureDefinition
    """Resolved infrastructure definition for this step, obtained by looking up the step's :attr:`infrastructure` pointer in the appropriate registry."""


class StepConfigRegistry(BaseModel):
    """Registry for step configurations, which can be referenced by the DAG to construct the pipeline."""

    steps: dict[str, StepConfig]
    """Mapping of step IDs to step configurations to execute in the pipeline."""

    def get(self, step_id: str) -> StepConfig:
        """Get a step configuration by step ID.

        Args:
            step_id: The ID of the step configuration to retrieve.

        Returns:
            Step: The step configuration with the specified step ID.

        Raises:
            ValueError: If no step configuration with the specified step ID is found.
        """
        if step_id not in self.steps:
            raise ValueError(f"Step configuration '{step_id}' not found in registry.")
        return self.steps[step_id]

    def values(self) -> Iterator[StepConfig]:
        """Iterate over the step configurations in the registry."""
        return iter(self.steps.values())
