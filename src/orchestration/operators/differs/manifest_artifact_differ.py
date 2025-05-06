import logging

from google.api_core.exceptions import NotFound as GCSNotFound
from google.cloud.storage import Client

from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig
from orchestration.utils.path import GCSPath, IOManager


class ManifestArtifactDiffer:
    """Check whether the artifacts in the manifest exist in the release_uri.

    This class fetches the manifest from GCS, using either `dev_uri` or `release_uri`
    and then checks the steps' artifacts' destinations to ensure they are present.

    Args:
        project_id (str): The GCP project ID. Defaults to the platform project.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def is_diff(self, *, step_name: str, config: UnifiedPipelineConfig, client: Client) -> bool:
        """List the artifacts in the manifest and check if they exist.

        Args:
            step_name (str): The name of the step to compare.
            config (UnifiedPipelineConfig): The unified pipeline configuration.
            client (Client): The Google Cloud Storage client used in the differ.

        Returns:
            bool: Whether the artifacts exist.
        """
        manifest_uri = config.manifest_uri()
        self.logger.info(f"downloading manifest from {manifest_uri}")
        m = IOManager().resolve(path=manifest_uri)
        if client and isinstance(m, GCSPath):
            m._client = client

        try:
            manifest = m.load()
        except GCSNotFound:
            self.logger.info("manifest not found")
            return True

        for artifact in manifest.get("steps", {}).get(step_name, {}).get("artifacts", []):
            artifact_uri: str = artifact.get("destination")
            self.logger.info(f"checking artifact {artifact_uri}")
            if not artifact_uri:
                self.logger.warning(f"artifact {artifact} has no destination")
                return True

            bucket, blob_name = artifact_uri.removeprefix("gs://").split("/", 1)
            b = client.bucket(bucket).blob(blob_name)
            if not b.exists():
                self.logger.warning(f"artifact {artifact_uri} not found")
                return True

        # if we reach this, all artifacts exist
        return False
