"""Staging configuration parser."""

from pydantic import BaseModel, field_validator

GENTROPY_STEPS = ["gwas_catalog_top_hit_ingestion", "ld_based_clumping", "pics"]


class StepConfig(BaseModel):
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
    prerequisites: list[str] = []
    params: dict[str, str] = {}

    @field_validator("params", mode="after")
    @classmethod
    def validate_params(cls, params: dict[str, str]) -> dict[str, str]:
        """Validate parameters."""
        # Example transformation: ensure paths are absolute or formatted correctly
        for key, value in params.items():
            if not key.startswith("step."):
                raise ValueError(f"Parameter '{key}' must start with 'step.'")

            if key == "step.session.write_mode":
                if value not in ["overwrite", "append"]:
                    raise ValueError(f"Invalid write mode: {value}")

            if key == "step.session.spark_uri":
                # Assuming spark_uri should be a valid URI
                if not "yarn" and not value.startswith("local"):
                    raise ValueError(f"Invalid spark URI: {value}")

            if key == "step":
                # Ensure step names are valid
                if value not in GENTROPY_STEPS:
                    raise ValueError(f"Invalid step name: {value}")
        return params


class DataprocClusterConfig(BaseModel):
    """Configuration for the DataProc cluster."""

    cluster_name: str
    cluster_metadata: dict[str, str]
    autoscaling_policy: str


class ConfigNodes(BaseModel):
    """Configuration for the nodes in the staging DAG."""

    nodes: list[StepConfig]
    cluster: DataprocClusterConfig


class StagingConfig:
    def __init__(self, raw_config: str):
        config_path

    def parser(self) -> None:
        """Parse the raw configuration string."""

    def _render(self) -> None:
        if not self.is_rendered:
            self.parser()
            self.is_rendered = True
