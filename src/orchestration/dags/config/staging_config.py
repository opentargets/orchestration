"""Staging configuration parser."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, field_validator

from orchestration.dags.config.app_config import AppConfig

GENTROPY_STEPS = ["gwas_catalog_top_hit_ingestion", "ld_based_clumping", "pics"]

BASE_TYPE_DICT = dict[str, str | int | float | bool | list[str]]


class StepConfigModel(BaseModel):
    """Configuration for a step in the DAG.

    The base class for all steps in the staging DAGs.

    Each step must define:

    - `id`: Unique identifier for the step.
    - `prerequisites`: List of prerequisite step IDs that must be completed before this step
      can run.
    - `params`: Dictionary of parameters specific to the step, which may include paths,
      configurations, or other settings required for the step's execution.

    The `params` are steps native to the Gentropy.

    The following checks are performed on the `params`:

    - Each key must start with "step." to ensure it is recognized as a step parameter.
    - The `step.session.write_mode` must be either "overwrite" or "append".
    - The `step.session.spark_uri` must be a valid URI, starting with "yarn" or "local".
    - The `step` key must be one of the predefined GENTROPY_STEPS.
    """

    id: str
    """Unique identifier for the step."""
    prerequisites: list[str] = []
    """List of prerequisite step IDs that must be completed before this step can run. Defaults to empty list."""
    params: BASE_TYPE_DICT
    """Dictionary of parameters specific to the step run."""

    @field_validator("params", mode="after")
    @classmethod
    def validate_params(cls, params: BASE_TYPE_DICT) -> BASE_TYPE_DICT:
        """Validate parameters.

        Args:
            params (dict): Dictionary of parameters to validate.

        Raises:
            ValueError: If parameter value do not contain the `step` prefix.
                        If `step.session.write_mode` is not "overwrite" or "append".
                        If `step.session.spark_uri` is not "yarn".
                        If `step` is not one of the predefined GENTROPY_STEPS.
        """
        for key, value in params.items():
            if not key.startswith("step"):
                raise ValueError(f"Parameter '{key}' must start with `step`.")

            if key == "step.session.write_mode":
                if value not in ["overwrite", "append"]:
                    raise ValueError(f"Invalid write mode: {value}")

            if key == "step.session.spark_uri":
                if not isinstance(value, str) or not "yarn":
                    raise ValueError(f"Invalid spark URI: {value}")

            if key == "step":
                if value not in GENTROPY_STEPS:
                    raise ValueError(f"Invalid step name: {value}")
        return params


class StagingConfigModel(BaseModel):
    """Configuration for the nodes in the staging DAG.

    This model defines the full Staging DAG configuration (all possible fields).

    By default it expects only `steps` to be defined in the config file.
    """

    steps: list[StepConfigModel]
    """List of steps to run in the DAG."""


class StagingConfig:
    """Staging DAG configuration.

    This class is used to parse and validate the staging DAG configuration stored in YAML files
    under the `src/orchestration/dags/config/staging` directory.

    It reads the configuration from a YAML file, applies template context for dynamic values,
    and validates the configuration against the `StagingConfigModel`.

    The object uses the `AppConfig` class to handle the parsing, rendering with template context and
    validating with pydantic model to ensure the configuration adheres to the expected structure.
    """

    def __init__(self, dag_name: str) -> None:
        config_path = Path(__file__).parent / "staging"

        self.logger = logging.getLogger(__name__)
        """Logger instance to use through the configuration."""
        staging_config_path = Path(config_path) / f"{dag_name}.yaml"
        """Path to the staging DAG configuration."""
        template_context_path = Path(config_path) / "staging.yaml"
        """Path to the template context."""
        self.dag_name = dag_name
        """Name of the dag used as prefix for the task ids defined in the config."""
        self.tc = AppConfig.from_file(file_path=template_context_path)
        """Template context used for rendering the Staging DAG config."""
        self.clusters = AppConfig.from_file(
            file_path=config_path.parent / "clusters.yaml",
            template_context={"gentropy_version": self.tc.get("gentropy_version")},
        )
        """The cluster definitions."""
        self.st = AppConfig.from_file(
            file_path=staging_config_path, template_context=self.tc.config, model=StagingConfigModel
        )
        """Parsed and rendered Staging DAG config."""
        self.run_name = self.tc.get("run_name") or datetime.now().strftime("%Y%m%d-%H%M")
        """Used for labelling resources"""
        self.staging_dag_steps = self.st.get("steps")
        """Steps to run in the DAG."""
        self.logger.info(f"Extracted {len(self.staging_dag_steps)} steps from the AppConfig")
