from __future__ import annotations

import logging
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pendulum import today
from pydantic import BaseModel, field_validator

from orchestration.dags.config.app_config import AppConfig


class OtterTaskConfig(BaseModel):
    work_path: str | None = None
    log_level: Literal["DEBUG", "INFO"] = "INFO"
    scratchpad: dict[str, str] | None = None
    steps: dict[str, Any]  # We do not care about the values inside, just step names.

    @property
    def step_names(self) -> list[str]:
        return list(self.steps.keys())

    def get_step_config(self, s: str) -> OtterTaskConfig:
        filtered_steps = {s: self.steps.get(s)}
        assert len(filtered_steps.keys()) == 1, f"Found duplicates for step {s}"
        tasks = filtered_steps.get(s)
        assert tasks is not None, f"Empty step {s}."
        assert len(tasks) != 0, f"Empty task list for step {s}"
        return OtterTaskConfig(
            work_path=self.work_path,
            log_level=self.log_level,
            scratchpad=self.scratchpad,
            steps=filtered_steps,
        )


class ToolSpecs(BaseModel):
    name: str
    config_path: Path

    def get_config(self) -> OtterTaskConfig:
        ac = AppConfig.from_file(self.config_path, validator=OtterTaskConfig)
        assert ac.validated
        return ac.validated


class Tools(Enum):
    GENTROUTILS = ToolSpecs(name="gentroutils", config_path=Path(__file__).parent.parent / "tools/gentroutils.yaml")

    @classmethod
    def get(cls, v: str) -> ToolSpecs:
        match v:
            case cls.GENTROUTILS.value.name:
                return cls.GENTROUTILS.value
            case _:
                raise ValueError(f"Tool {v} not found in the registered tools")

    @classmethod
    def names(cls) -> list[str]:
        return [c.name for c in cls.__members__.values()]


class StagingPipelineConfig:
    staging_release: str = today("UTC").to_iso8601_string()

    def __init__(self, path: Path) -> None:
        self.logger = logging.getLogger(__name__)
        self.raw = AppConfig.from_file(path, validator=StagingPipelineConfigModel)
        if not self.raw.validated:
            raise ValueError("Validation failed for the staging pipeline configuration.")
        sentinels = self.raw.validated.sentinels
        self.logger.debug(f"Using sentinels: {sentinels}")
        # Since we have valid object and sentinels, now reread the config and use sentinels
        self.templated = AppConfig.from_file(path, validator=StagingPipelineConfigModel, template_context=sentinels)
        assert self.templated.validated
        for step in self.templated.validated.steps:
            self.logger.debug(f"Step: {step.name}, depends_on: {step.depends_on},  config: {step.step_config}")


class EnvironmentSpecs(BaseModel):
    name: Literal["prod", "test"]
    vars: dict[str, str] | None


class SoftwareVersionMap(BaseModel):
    vars: dict[str, str]


class GentroutilsStep(BaseModel):
    name: str
    params: dict[str, str]
    depends_on: list[str]


class Step(BaseModel):
    name: str
    depends_on: list[str] | None = None

    # Make sure it is validated after type is checked
    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        tool_name = v.split("_")[0]
        step_name = v[len(tool_name) + 1 :]
        if step_name not in Tools.get(tool_name).get_config().step_names:
            raise ValueError(f"Step name '{v}' is not a valid step for tool '{tool_name}'")
        return v

    @property
    def step_config(self):
        tool_name = self.name.split("_")[0]
        step_name = self.name[len(tool_name) + 1 :]
        return Tools.get(tool_name).get_config().get_step_config(step_name)


class StagingPipelineConfigModel(BaseModel):
    environment_specs: list[EnvironmentSpecs]
    env: Literal["prod", "dev"]
    steps: list[Step]

    @field_validator("environment_specs")
    @classmethod
    def validate_environment_specs_not_empty(cls, v):
        if not v:
            raise ValueError("environment_specs cannot be empty")
        return v

    @property
    def sentinels(self) -> dict[str, str]:
        i = 0
        while i < len(self.environment_specs):
            if self.environment_specs[i].name.lower() == self.env.lower():
                return self.environment_specs[i].vars or {}
            i += 1
        return {}
