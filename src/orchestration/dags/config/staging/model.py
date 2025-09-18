from typing import Literal, TypeVar

import yaml
from pydantic import BaseModel, field_validator

from orchestration.dags.config.tools import OtterTaskConfig, Tools

T = TypeVar("T", bound=BaseModel)


class EnvironmentSpecs(BaseModel):
    """Environment specification."""

    name: Literal["prod", "dev"]
    """Name of the environment. Can be 'prod' or 'dev'."""
    vars: dict[str, str] | None
    """Dictionary of variables to be used as sentinels in the configuration."""


class NodeInfrastructureModel(BaseModel):
    """Infrastructure specification for a pipeline node."""

    vm: dict[str, str] | None = None
    """VM configuration for the node."""
    batch: dict[str, str] | None = None
    """Batch configuration for the node."""
    cluster: dict[str, str] | None = None
    """Cluster configuration for the node."""


class StagingPipelineNodeModel(BaseModel):
    """Pipeline node specification."""

    name: str
    """Name of the node. Must be in the format <tool>_<step>, where <tool> is one of the registered tools"""
    depends_on: list[str] | None = None
    """Names of the nodes that this node depends on."""
    infrastructure: NodeInfrastructureModel
    """Infrastructure specification for the node."""
    steps: list[OtterTaskConfig]

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Validate the node name."""
        cls._validate_node_name(v)

    @field_validator("depends_on")
    @classmethod
    def validate_depends_on(cls, v):
        """Validate the depends_on field node names."""
        if v is None:
            return v
        if len(v) != len(set(v)):
            raise ValueError("Duplicate node names found in depends_on")
        for node in v:
            cls._validate_node_name(node)
        return v

    @property
    def runtime_config(self) -> str:
        tool_name = self.name.split("_")[0]
        step_name = self.name[len(tool_name) + 1 :]
        runtime_config: OtterTaskConfig = Tools.get(tool_name).get_config().get_step_config(step_name)
        runtime_config_dict = runtime_config.model_dump()

        return yaml.dump(runtime_config_dict, sort_keys=True)

    def __repr__(self) -> str:
        return f"Step(name={self.name}, depends_on={self.depends_on}, runtime_config={self.runtime_config})"

    @staticmethod
    def _validate_node_name(v: str) -> str:
        """Validate the node name.

        Args:
            v (str): Node name.

        Raises:
            ValueError: If the node name is not valid.

        Returns:
            str: The validated node name.
        """
        tool_name = v.split("_")[0]
        step_name = v[len(tool_name) + 1 :]
        if step_name not in Tools.get(tool_name).get_config().step_names:
            raise ValueError(f"Step name '{v}' is not a valid step for tool '{tool_name}'")
        return v


class StagingPipelineConfigModel(BaseModel):
    """Staging pipeline configuration model.

    Example:
    ```yaml
    environment_specs:
      - name: prod
        vars:
          DATA_VERSION: "23.06"
          RELEASE: "2024-06"
      - name: test
        vars:
          DATA_VERSION: "23.06"
          RELEASE: "staging-test"
    env: prod
    steps:
      - name: gentroutils_download
        depends_on: []
      - name: gentroutils_process
        depends_on:
          - gentroutils_download
    ```
    """

    environment_specs: list[EnvironmentSpecs]
    """List of environment specifications."""
    env: Literal["prod", "dev"]
    """Environment to use. Can be 'prod' or 'dev'."""
    steps: list[Step]
    """Steps to include in the pipeline run. Must be valid step names registered in the tools."""
    release_uri: str
    """URI to the release info location."""
    staging_bucket: str
    """GCS bucket to use for staging."""

    @field_validator("staging_bucket")
    @classmethod
    def validate_staging_bucket(cls, v: str) -> str:
        """Validate that the staging bucket is a GCS bucket."""
        if not v.startswith("gs://"):
            raise ValueError("staging_bucket must be a GCS bucket (start with gs://)")
        return v

    @field_validator("environment_specs")
    @classmethod
    def validate_environment_specs_not_empty(cls, v):
        """Validate that environment_specs is not empty."""
        if not v:
            raise ValueError("environment_specs cannot be empty")
        return v

    @property
    def sentinels(self) -> dict[str, str]:
        """Get the sentinels for the current environment."""
        i = 0
        while i < len(self.environment_specs):
            if self.environment_specs[i].name.lower() == self.env.lower():
                return self.environment_specs[i].vars or {}
            i += 1
        return {}

    def step_specific_config(self, step_name: str) -> dict[str, str]:
        """Get the specific configuration for a step.

        Args:
            step_name (str): Name of the step.

        Returns:
            dict[str, str] | None: The specific configuration for the step, or None if not found.
        """
        for step in self.steps:
            if step.name == step_name:
                return step.params or {}
        raise ValueError(f"Step name '{step_name}' not found in steps.")
