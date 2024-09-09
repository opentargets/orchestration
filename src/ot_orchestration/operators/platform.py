"""Custom operators for the Platform part of the pipeline."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Iterable

import yaml
from airflow.operators.branch import BaseBranchOperator
from airflow.utils.context import Context
from google.api_core.exceptions import NotFound
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from ot_orchestration.utils import GCSPath, read_yaml_config
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
        local_config_path: The path to the local configuration file.
        upstream_config_url: The URL of the upstream configuration file.
    """

    template_fields: Sequence[str] = (
        "step_name",
        "local_config_path",
        "upstream_config_url",
    )

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        step_name: str,
        local_config_path: Path,
        upstream_config_url: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.step_name = step_name
        self.project_id = project_id
        self.local_config_path = local_config_path
        self.upstream_config_url = upstream_config_url

    def get_local_config(self) -> dict[str, Any]:
        """Read the local configuration file and return the relevant part."""
        return read_yaml_config(self.local_config_path)

    def get_upstream_config(self, url: str) -> dict[str, Any]:
        """Download the upstream configuration file and return the relevant part."""
        c = Client(self.project_id)
        bucket_name, bucket_path = GCSPath(url).split()
        b = Bucket(client=c, name=bucket_name)
        blob = b.blob(bucket_path)
        upstream_config = blob.download_as_bytes()
        return yaml.safe_load(upstream_config)

    def extract_relevant_config(self, config: dict[str, Any]) -> dict[str, Any]:
        """Get the relevant part of the configuration for the current step."""
        return {
            "scratchpad": config.get("scratchpad", None),
            "step": config.get("steps", {}).get(self.step_name, None),
        }

    def choose_branch(self, context: Context) -> str | Iterable[str]:
        """Decide whether to run the current PIS step or not."""
        local_config = self.get_local_config()
        upstream_config = {}

        try:
            upstream_config = self.get_upstream_config(self.upstream_config_url)
        except NotFound:
            self.log.info(
                "Upstream configuration file not found, assuming first run, %s",
                self.upstream_config_url,
            )

        lc = self.extract_relevant_config(local_config)
        uc = self.extract_relevant_config(upstream_config)

        self.log.info(f"Local config: {lc}")
        self.log.info(f"Upstream config: {uc}")

        should_run = lc["scratchpad"] != uc["scratchpad"] or lc["step"] != uc["step"]

        if should_run:
            self.log.info("Configuration differs, step %s will run", self.step_name)
            return f"pis_{self.step_name}.upload_config_{self.step_name}"

        self.log.info("Configuration is equal, step %s will not run", self.step_name)
        return f"pis_{self.step_name}.join_{self.step_name}"
