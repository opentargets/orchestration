import logging

from google.api_core.exceptions import NotFound as GCSNotFound
from google.cloud.storage import Client

from orchestration.dags.config.app_config import AppConfig
from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig


class ConfigDiffer:
    """Check whether the configuration for a step has changed or not.

    This class fetches the config from GCS and checks if it is less or equal to
    the local config.

    Args:
        project_id (str): The GCP project ID. Defaults to the platform project.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def is_diff(self, *, step_name: str, config: UnifiedPipelineConfig, client: Client) -> bool:
        """Compare the local configuration with the remote configuration.

        Args:
            step_name (str): The name of the step to compare.
            config (UnifiedPipelineConfig): The unified pipeline configuration.
            client (Client): The Google Cloud Storage client used in the differ.

        Returns:
            bool: Whether the configs are different.
        """
        stage, _ = step_name.split("_", 1)

        try:
            local_config = getattr(config, stage)
        except AttributeError:
            self.logger.warning(f"local config not found for step {step_name}")
            return True

        try:
            remote_config_path = config.config_uri(step_name)
            remote_config = AppConfig.from_file(
                file_path=remote_config_path,
                template_context=local_config.template_context,
                client=client,
            )
        except GCSNotFound:
            self.logger.info(f"remote config not found: {remote_config_path}")
            return True

        self.logger.info(f"comparing local config for step {step_name} with {remote_config_path}")

        # if the local config does not contain the remote config, the differ triggers
        return not local_config <= remote_config
