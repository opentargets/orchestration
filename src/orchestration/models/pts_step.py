from __future__ import annotations

from pathlib import Path

from google.cloud.dataproc_v1.types.jobs import PySparkJob

from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig
from orchestration.models.step import UnifiedPipelineStep
from orchestration.operators.dataproc import PTSJobBuilder

ASSET_PATH = Path(__file__).parent.parent / "assets"
PTS_CLUSTER_NAME = "pts"


def pts_step_from_config(
    name: str,
    config: UnifiedPipelineConfig,
) -> PTSStep:
    """Factory function to create a PTSStep from config.

    Args:
        name (str): The name of the step.
        config (UnifiedPipelineConfig): The config object for PTS.

    Returns:
        PTSStep: The created PTSStep instance.
    """
    short_name = name.split("_", 1)[1]
    step_tasks = config.pts.config.get("steps", {}).get(short_name, {})
    if len(step_tasks) == 0:
        raise ValueError(f"no tasks found for pts step {name}")

    # if any task inside a step is a pyspark task, then this is a dataproc step
    for task in step_tasks:
        if task.get("name", "").startswith("pyspark"):
            return PTSDataprocStep(name, config)

    return PTSStep(name, config)


class PTSStep(UnifiedPipelineStep):
    """PTS step class.

    The PTSStep class represents a PTS step in the Unified Pipeline orchestration
    DAG.

    This is also a stub for now, most functionality currently implemented is related
    to dataproc steps and their infrastructure.
    """

    def __init__(
        self,
        name: str,
        config: UnifiedPipelineConfig,
    ):
        """Initialize a PTSStep instance.

        Args:
            name (str): The name of the step.
            config (AppConfig): The config object for PTS.
            logger (logging.Logger, optional): Logger instance for logging. Defaults to None.
        """
        super().__init__(name, config)

    @property
    def is_dataproc(self) -> bool:
        """Returns whether the step is a Dataproc step."""
        return type(self) is PTSDataprocStep

    @property
    def is_gce(self) -> bool:
        """Returns whether the step is a GCE step."""
        return type(self) is PTSStep

    @property
    def config_uri(self) -> str:
        """Returns the URI for the step config."""
        return f"{self.config_destination_prefix}/{self.name}.yaml"


class PTSDataprocStep(PTSStep):
    """PTS Dataproc step class.

    The PTSDataprocStep class represents a PTS step that runs on a Dataproc cluster.
    """

    def __init__(
        self,
        name: str,
        config: UnifiedPipelineConfig,
    ):
        """Initialize a PTSDataprocStep instance.

        Args:
            name (str): The name of the step.
            config (AppConfig): The config object for PTS.
        """
        super().__init__(name, config)
        self.runs_on_cluster = True
        self.cluster_definition = self.get_cluster_definition(name)
        if not self.is_dataproc:
            raise ValueError(f"step {name} is not intended to run in dataproc")

    @property
    def dataproc_script_run_source(self) -> Path:
        """Returns the path to the Dataproc run script, if applicable."""
        return ASSET_PATH / "dataproc_pts_run.py"

    @property
    def dataproc_script_run_uri(self) -> str:
        """Returns the URI for the Dataproc run script, if applicable."""
        return f"{self.bin_destination_prefix}/{self.name}_dataproc_pts_run.py"

    @property
    def dataproc_args(self) -> list[str]:
        return ["-s", self.short_name, "-c", f"{self.name}.yaml"]

    def build_job(self) -> PySparkJob:
        """Builds the job for the step."""
        self.logger.info(
            f"Debug - dataproc_script_run_uri: {self.dataproc_script_run_uri}, type: {type(self.dataproc_script_run_uri)}"
        )
        self.logger.info(f"Debug - dataproc_args: {self.dataproc_args}, type: {type(self.dataproc_args)}")
        self.logger.info(f"Debug - config_uri: {self.config_uri}, type: {type(self.config_uri)}")

        if not self.is_dataproc:
            raise NotImplementedError("called build_job on non-dataproc step")

        return PTSJobBuilder(
            main_python_file_uri=self.dataproc_script_run_uri,
            args=self.dataproc_args,
            config_uri=self.config_uri,
        ).build()
