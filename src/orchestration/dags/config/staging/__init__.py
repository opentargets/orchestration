from __future__ import annotations

import logging
from pathlib import Path

from pendulum import today

from orchestration.dags.config.app_config import AppConfig
from orchestration.dags.config.staging.model import StagingPipelineConfigModel
from orchestration.dags.config.unified_pipeline import ClusterDefinition


class StagingPipelineConfig:
    """Staging pipeline configuration."""

    release_date: str = today("UTC").to_iso8601_string()
    """Release date in ISO 8601 format."""

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
            self.logger.debug(step)

    def step_cluster_definition(self, step_name: str) -> ClusterDefinition | None:
        """Return the cluster type and configuration for a step.

        This method finds the proper cluster definition by matching on the most
        specific cluster name that is a prefix of the step name. So if the step
        name is `pis_foo_bar`, and the cluster names are `pis_foo_` and `pis_`,
        the cluster definition for `pis_foo_` will be returned.

        A step can also be configured to not use a cluster by setting the
        `cluster` key to `False` in the step configuration. In this case, the
        method will return None. This is useful for Gentropy steps that are run
        using Google Batch.

        Args:
            step_name (str): The name of the step, in the form `{stage}_{step_name}`.

        Returns:
            ClusterDefinition | None: A ClusterDefinition object containing the
                cluster type and configuration for the step. If the step requires
                no cluster, returns None.

        Raises:
            ValueError: If no cluster definition is found for the step name.
        """
        assert self.templated.validated, "StagingPipelineConfig must be validated."

        clusters = self.clusters.config.get("clusters", {})
        sorted_cluster_names = sorted(clusters.keys(), key=len, reverse=True)
        for cluster_name in sorted_cluster_names:
            if step_name.startswith(cluster_name):
                return ClusterDefinition(cluster_name, clusters[cluster_name])
        raise ValueError(f"No cluster definition found for step {step_name}.")

    @property
    def clusters(self) -> AppConfig:
        """Return cluster configurations."""
        return AppConfig.from_file(Path(__file__).parent.parent / "infrastructure" / "clusters.yaml")

    @property
    def step_config_upload_path(self) -> dict[str, str]:
        """Return the upload path for each step."""
        assert self.templated.validated, "StagingPipelineConfig must be validated."
        steps = self.templated.validated.steps
        upload_paths = {}
        for step in steps:
            path = self.templated.validated.staging_bucket + "/" + step.name + "/" + self.release_date
            upload_paths[step.name] = path
        return upload_paths

    @property
    def step_config_tools(self) -> dict[str, str]:
        """Return the tools for each step."""
        assert self.templated.validated, "StagingPipelineConfig must be validated."
        steps = self.templated.validated.steps
        tools = {}
        for step in steps:
            tools[step.name] = step.name.split("_")[0]
        return tools
