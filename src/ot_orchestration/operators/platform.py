"""Custom operators for the Platform part of the pipeline."""

from collections.abc import Sequence
from typing import Any, Iterable

import yaml
from airflow.operators.branch import BaseBranchOperator
from airflow.utils.context import Context
from google.api_core.exceptions import NotFound
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from ot_orchestration.utils import GCSPath
from ot_orchestration.utils.common import GCP_PROJECT_PLATFORM


class PISDiffComputeOperator(BaseBranchOperator):
    """Custom operator that decides whether to run a PIS step or not.

    At the moment, this operator will check the parts of the PIS configuration
    that are relevant to the current step, comparing them against the copy in the
    specified bucket. If they are different, that means the step should be
    run.

    It would be interesting add a check that downloads the manifest from the
    specified bucket and compares the resources listed for the step with the
    files in the bucket, along with their checksums. That way we can ensure the
    files exist and have not been tampered with.

    Args:
        project_id: The GCP project ID. Defaults to the platform project.
        step_name: The name of the PIS step to check.
        local_config: The local configuration.
        remote_config_url: The URL of the remote configuration file.
    """

    template_fields: Sequence[str] = (
        "step_name",
        "local_config",
        "remote_config_url",
    )

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        step_name: str,
        local_config: dict[str, Any],
        remote_config_url: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.step_name = step_name
        self.project_id = project_id
        self.local_config = local_config
        self.remote_config_url = remote_config_url

    def get_remote_config(self, url: str) -> dict[str, Any]:
        """Download the remote configuration file and return the relevant part."""
        c = Client(self.project_id)
        bucket_name, bucket_path = GCSPath(url).split()
        b = Bucket(client=c, name=bucket_name)
        blob = b.blob(bucket_path)
        remote_config = blob.download_as_bytes()
        return yaml.safe_load(remote_config)

    def extract_relevant_config(self, config: dict[str, Any]) -> dict[str, Any]:
        """Get the relevant part of the configuration for the current step."""
        return {
            "scratchpad": config.get("scratchpad", None),
            "step": config.get("steps", {}).get(self.step_name, None),
        }

    def choose_branch(self, context: Context) -> str | Iterable[str]:
        """Decide whether to run the current PIS step or not."""
        remote_config = {}

        try:
            remote_config = self.get_remote_config(self.remote_config_url)
        except NotFound:
            self.log.info(
                "remote configuration file not found, assuming first run, %s",
                self.remote_config_url,
            )

        lc = self.extract_relevant_config(self.local_config)
        uc = self.extract_relevant_config(remote_config)

        self.log.info(f"local  config: {lc}")
        self.log.info(f"remote config: {uc}")

        should_run = lc["scratchpad"] != uc["scratchpad"] or lc["step"] != uc["step"]

        if should_run:
            self.log.info("configuration differs, step %s will run", self.step_name)
            return f"pis_stage.pis_{self.step_name}.upload_config_{self.step_name}"

        self.log.info("configuration is equal, step %s will not run", self.step_name)
        return f"pis_stage.pis_{self.step_name}.join_{self.step_name}"
