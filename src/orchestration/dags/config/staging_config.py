"""Staging DAG configuration."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from orchestration.dags.config.app_config import AppConfig
from orchestration.models.environment import Environments
from orchestration.utils.common import GCP_PROJECT_GENETICS

if TYPE_CHECKING:
    from typing import Any


class NoStepsFoundError(Exception):
    """Custom exception for when no steps are found in the staging pipeline configuration."""

    def __init__(self, message: str):
        super().__init__(message)


class StepConfigParseError(Exception):
    """Custom exception for errors during parsing of step configurations."""

    def __init__(self, message: str, step_config: dict[str, Any]):
        super().__init__(message)
        self.step_config = step_config


class StagingPipelineConfig:
    """Configuration for the staging pipeline."""

    def __init__(self, config_path: Path):
        self.logger = logging.getLogger(__name__)
        """Logger for the staging pipeline configuration."""

        # Pre-load the configuration and find the environment variables to apply using the templating context.
        pre_st = AppConfig.from_file(file_path=config_path)

        self.env = pre_st.get("env", "Test")
        """Environment to run the pipeline in, e.g. "Prod", "Test", etc. The environment is used to derive the templating context to pre-fill the configuration."""
        self.environments = Environments(environments=pre_st.get("environments", []))
        """List of environment specifications for the staging pipeline, including environment variable values that can be used in the pipeline's tasks."""
        # Actual configuration with templating applied, used for the rest of the DAG
        st = AppConfig.from_file(file_path=config_path, template_context=self.environments.get_vars(self.env))
        self.project_id: str = st.get("project_id", GCP_PROJECT_GENETICS)
        """GCP project to run the pipeline in."""
        self.is_staging = True
        """Marks the configuration as staging pipeline."""
        self._steps: list[dict[str, Any]] = st.get("steps", [])
        """Pipeline steps to execute."""
        self.run_name: str = st.get("run_name") or datetime.now().strftime("%Y%m%d%H%M%S")
        """Used for labeling resources created by the DAG."""
        self.datasource: str = st.get("datasource", "unknown_datasource")
        """Name of the datasource being ingested, e.g. "decode_protein_summary_stats"."""
        self.service_account_extra_scopes: list[str] = st.get("service_account_extra_scopes", [])
        """Additional scopes to add to the service account used by the DAG's tasks, e.g. for accessing other GCP services."""
        self._clusters: dict[str, Any] = st.get("clusters", [])
        """Cluster configuration for Dataproc clusters created by the DAG."""
        self._batch_jobs: dict[str, Any] = st.get("batch_jobs", [])
        """Batch job configuration for the DAG."""
        self._required_cluster_names: set[str] = set()
        """Required cluster names. This is populated during step parsing."""
        self._required_batch_job_names: set[str] = set()
        """Required batch job names. This is populated during step parsing."""
