"""Unified Pipeline step class definition."""

from __future__ import annotations

import logging
from abc import abstractmethod
from enum import StrEnum

from orchestration.dags.config.unified_pipeline import ClusterDefinition, UnifiedPipelineConfig


class UnifiedPipelineStage(StrEnum):
    """Enum representing different stages in the unified pipeline."""

    PIS = "pis"
    PTS = "pts"
    ETL = "etl"
    GENTROPY = "gentropy"

    @classmethod
    def from_step_name(
        cls,
        step_name: str,
    ) -> UnifiedPipelineStage:
        """Returns the UnifiedPipelineStage corresponding to the given step name."""
        step_prefix = step_name.split("_")[0].lower()
        return UnifiedPipelineStage(step_prefix)


class UnifiedPipelineStep:
    """Unified Pipeline step class.

    The Step class represents a step in an orchestration workflow.

    This class is a stub for now, meant to be expanded with additional functionality
    ported from other places in the orchestration codebase.

    It is meant to be subclassed by more specific step implementations.
    """

    def __init__(
        self,
        name: str,
        config: UnifiedPipelineConfig,
    ):
        """Initialize a Step instance.

        Args:
            name (str): The name of the step.
            config (AppConfig): The config object for the application the step uses.
            logger (logging.Logger, optional): Logger instance for logging. Defaults to None.
        """
        self.name = name
        """The full name of the step, including stage prefix."""
        self.short_name = name.split("_", 1)[1]
        """The short name of the step, without stage prefix."""
        self.config = config
        """The UnifiedPipelineConfig."""
        self.stage = UnifiedPipelineStage.from_step_name(name)
        """The stage of the unified pipeline this step belongs to."""
        self.runs_on_cluster = False
        """Whether the step runs on a cluster."""
        self.cluster_definition: ClusterDefinition
        """The cluster definition for the step."""
        self.logger = logging.getLogger(__name__)

        self.logger.info(f"initializing {self.stage} step: {self.name}")

    @property
    def config_destination_prefix(self) -> str:
        """Returns the URI prefix where the step config should be uploaded."""
        return f"{self.config.dev_uri or self.config.release_uri}/etc/config"

    @property
    def bin_destination_prefix(self) -> str:
        """Returns the URI prefix where the step binaries should be uploaded."""
        return f"{self.config.dev_uri or self.config.release_uri}/etc/bin"

    @property
    @abstractmethod
    def config_uri(self) -> str:
        """Returns the URI for the step config."""

    def get_cluster_definition(self, step_name) -> ClusterDefinition:
        """Returns the cluster definition for the step, if applicable.

        Raises:
            ValueError: If the step does not run on a cluster.
        """
        if self.runs_on_cluster is False:
            raise ValueError(f"step {step_name} does not run on a cluster.")

        clusters = self.config.clusters.config.get("clusters", {})
        sorted_cluster_names = sorted(clusters.keys(), key=len, reverse=True)
        for cluster_name in sorted_cluster_names:
            if step_name.startswith(cluster_name):
                return ClusterDefinition(cluster_name, clusters[cluster_name])
        raise ValueError(f"no cluster definition found for step {step_name}.")
