"""Configuration for the gentropy pipelines."""

from __future__ import annotations

import logging
import re
from collections import UserString
from pathlib import Path

from orchestration.dags.config.app_config import AppConfig
from orchestration.models.dataproc import DataprocConfig
from orchestration.models.gentropy import GentropyCommandConfig
from orchestration.models.otter import OtterCommandConfig
from orchestration.models.staging_pipeline import StagingPipelineConfig


class GentropyPipelineConfig:
    """Configuration class for Gentropy pipeline."""

    registered_tools = ["gentroutils", "gentropy"]

    def __init__(self, st: StagingPipelineConfig, resource_prefix: str) -> None:
        self.logger = logging.getLogger(__name__)

        self.prefix = resource_prefix
        """Prefix to use for resource names."""
        self.logger.info(f"Using resource prefix: {self.prefix}")

        self.env = st.env
        """Environment to run the steps in."""
        self.logger.info(f"Initialized GentropyPipelineConfig with env: {self.env}")

        self.sentinels = st.get_sentinels_for_env()
        """Sentinels dict to replace the placeholders in the configuration."""
        self.logger.info(f"Using sentinels: {self.sentinels}")

        self.staging_bucket = st.staging_bucket
        """Staging bucket to store intermediate files."""
        self.logger.info(f"Using staging bucket: {self.staging_bucket}")

        self.gentropy_runtime_config = self.read_gentropy_command_config(self.sentinels)
        """Configuration for gentropy steps."""
        self.logger.info("Loaded gentropy runtime configuration")

        self.gentroutils_runtime_config = self.read_gentroutils_command_config(self.sentinels)
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
    def read_config(cls, path: str, resource_prefix: str) -> GentropyPipelineConfig:
        """Read the configuration from a yaml file.

        This constructor method reads the configuration file twice.
        The first time it reads the file to get the sentinels for the selected
        environment. The second time it reads the file with the sentinels to
        get the final configuration.


        Args:
            path (str): Path to the configuration file.
            resource_prefix (str): Prefix to use for the resource names.

        Returns:
            GentropyPipelineConfig: An instance of the GentropyPipelineConfig class.
        """
        config_path = cls._ensure_path(path)
        # Validate the config, so we are sure to work with the instance of StagingPipelineConfig
        config = AppConfig.from_file(config_path).validate(StagingPipelineConfig)
        sentinels = config.get_sentinels_for_env()
        config = AppConfig.from_file(config_path, template_context=sentinels).validate(StagingPipelineConfig)
        return cls(config, resource_prefix)

    @classmethod
    def read_gentropy_command_config(cls, sentinels: dict[str, str]) -> GentropyCommandConfig:
        """Read the configuration for gentropy steps.

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
    def read_gentroutils_command_config(cls, sentinels: dict[str, str]) -> OtterCommandConfig:
        """Read the configuration for gentroutils steps.

        This constructor method reads the gentroutils configuration file.

        Returns:
            OtterCommandConfig: An instance of the OtterConfig class.
        """
        config_path = Path(__file__).parent / "gentroutils.yaml"
        return AppConfig.from_file(config_path, template_context=sentinels).validate(OtterCommandConfig)
