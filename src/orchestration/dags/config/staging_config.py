"""Configuration for the gentropy pipelines."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal, Self

# from airflow.models.taskmixin import DAGNode
from pydantic import BaseModel, field_validator, model_validator

from orchestration.dags.config.app_config import AppConfig
from orchestration.operators.dataproc import ClusterConfig

# class RuntimeStep(BaseModel):
#     """Pydantic model for a runtime step."""

#     name: str
#     """Name of the step."""

#     dag_node: DAGNode | None = None
#     """DAG Node, can be an operator or a task group."""

#     depends_on: list[str] | None = None

#     @property
#     def tool(self) -> str:
#         """Get the tool name from the step name."""
#         return self.name.split("_")[0]


class OtterCommandConfig(BaseModel):
    """Pydantic model for Otter based configurations."""

    work_path: str | None = None
    """Path to the work directory."""
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "SUCCESS"] = "INFO"
    """Log level for the otter steps."""
    scratchpad: dict[str, str] | None = None
    """Scratchpad with replacements for the otter steps."""
    steps: dict[str, list[Any]]
    """List of otter steps"""


class GentropyCommandConfig(BaseModel):
    """Pydantic model for Gentropy steps command configuration."""

    steps: dict[str, dict[str, Any]]
    """Steps available in the gentropy runtime configuration."""


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


class GentropyPipelineConfig:
    """Configuration class for Gentropy pipeline."""

    def __init__(self, st: StagingPipelineConfig) -> None:
        self.logger = logging.getLogger(__name__)

        self.env = st.env
        """Environment to run the steps in."""
        self.logger.info(f"Initialized GentropyPipelineConfig with env: {self.env}")

        self.sentinels = st.get_sentinels_for_env()
        """Sentinels dict to replace the placeholders in the configuration."""
        self.logger.info(f"Using sentinels: {self.sentinels}")

        self.staging_bucket = st.staging_bucket
        """Staging bucket to store intermediate files."""
        self.logger.info(f"Using staging bucket: {self.staging_bucket}")

        self.gentropy_runtime_config = self.read_gentropy_runtime_config(self.sentinels)
        """Configuration for gentropy steps."""
        self.logger.info("Loaded gentropy runtime configuration")

        self.gentroutils_runtime_config = self.read_gentroutils_runtime_config(self.sentinels)
        """Configuration for gentroutils steps."""
        self.logger.info("Loaded gentroutils runtime configuration")

        self.dataproc_config = self.read_dataproc_config(self.sentinels)
        """Configuration for dataproc clusters."""
        self.logger.info("Loaded dataproc configuration")

        self.steps = st.steps
        """Steps to execute in the pipeline."""
        self.logger.info(f"Loaded {len(self.steps)} steps to execute in the pipeline")

    @staticmethod
    def _ensure_path(path: str) -> Path:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config path {path} does not exist")
        if not p.is_file():
            raise ValueError(f"Config path {path} is not a file")
        if p.suffix not in [".yaml", ".yml"]:
            raise ValueError(f"Config path {path} is not a yaml file")
        return p

    @classmethod
    def read_config(cls, path: str) -> GentropyPipelineConfig:
        """Read the configuration from a yaml file.

        This constructor method reads the configuration file twice.
        The first time it reads the file to get the sentinels for the selected
        environment. The second time it reads the file with the sentinels to
        get the final configuration.


        Args:
            path (str): Path to the configuration file.

        Returns:
            GentropyPipelineConfig: An instance of the GentropyPipelineConfig class.
        """
        config_path = cls._ensure_path(path)
        # Validate the config, so we are sure to work with the instance of StagingPipelineConfig
        config = AppConfig.from_file(config_path).validate(StagingPipelineConfig)
        sentinels = config.get_sentinels_for_env()
        config = AppConfig.from_file(config_path, template_context=sentinels).validate(StagingPipelineConfig)
        return cls(config)

    @classmethod
    def read_gentropy_runtime_config(cls, sentinels: dict[str, str]) -> GentropyCommandConfig:
        """Read the runtime configuration for gentropy steps.

        This constructor method reads the gentropy runtime configuration file.

        Returns:
            GentropyCommandConfig: An instance of the GentropyRuntimeConfig class.
        """
        config_path = Path(__file__).parent / "gentropy.yaml"
        return AppConfig.from_file(config_path, template_context=sentinels).validate(GentropyCommandConfig)

    @classmethod
    def read_dataproc_config(cls, sentinels: dict[str, str]) -> DataprocConfig:
        """Read the cluster configuration for dataproc clusters.

        This constructor method reads the dataproc clusters configuration file.

        Returns:
            ClusterConfig: An instance of the ClusterConfig class.
        """
        config_path = Path(__file__).parent / "clusters.yaml"
        return AppConfig.from_file(config_path, template_context=sentinels).validate(DataprocConfig)

    @classmethod
    def read_gentroutils_runtime_config(cls, sentinels: dict[str, str]) -> OtterCommandConfig:
        """Read the configuration for gentroutils steps.

        This constructor method reads the gentroutils configuration file.

        Returns:
            OtterCommandConfig: An instance of the OtterConfig class.
        """
        config_path = Path(__file__).parent / "gentroutils.yaml"
        return AppConfig.from_file(config_path, template_context=sentinels).validate(OtterCommandConfig)


# class Chainer:
#     """Utility class to chain tasks together based on their dependencies."""

#     def __init__(self) -> None:
#         self.logger = logging.getLogger(__name__)
#         self.logger.info("Initialized Chainer")

#     def chain_tasks(self, tasks: list[RuntimeStep], nodes: list[DAGNode]) -> None:
#         pass
