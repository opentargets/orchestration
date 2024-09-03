"""Custom operators for the Platform part of the pipeline."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Iterable

import yaml
from airflow.exceptions import AirflowException
from airflow.operators.branch import BaseBranchOperator
from airflow.utils.context import Context
from google.api_core.exceptions import NotFound
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from ot_orchestration.utils.utils import bucket_name, bucket_path, read_yaml_config


class PISDiffComputeOperator(BaseBranchOperator):
    """Custom operator that decides whether to run a PIS step or not.

    This operator will check the part of the PIS configuration that is relevant
    to the current step, comparing it against the copy in the specified bucket.
    If the two are different, that means the step should be run. Otherwise, the
    operator will branch to the end of the DAG.
    """

    template_fields: Sequence[str] = (
        "step_name",
        "local_config_path",
        "upstream_config_url",
        "downstream_task_id",
        "joiner_task_id",
    )

    def __init__(
        self,
        *args,
        project_id: str,
        step_name: str,
        local_config_path: Path,
        upstream_config_url: str,
        downstream_task_id: str,
        joiner_task_id: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.step_name = step_name
        self.project_id = project_id
        self.local_config_path = local_config_path
        self.upstream_config_url = upstream_config_url
        self.downstream_task_id = downstream_task_id
        self.joiner_task_id = joiner_task_id

    def get_local_config(self) -> dict[str, Any]:
        """Read the local configuration file and return the relevant part."""
        return read_yaml_config(self.local_config_path)

    def get_upstream_config(self, url: str) -> dict[str, Any]:
        """Download the upstream configuration file and return the relevant part."""
        c = Client(self.project_id)
        b = Bucket(client=c, name=bucket_name(url))
        blob = b.blob(bucket_path(url))
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
        upstream_config = None

        try:
            upstream_config = self.get_upstream_config(self.upstream_config_url)
        except NotFound:
            self.log.info(
                "Upstream configuration file not found, assuming first run, %s",
                self.upstream_config_url,
            )
            raise

        l = self.extract_relevant_config(local_config)
        u = self.extract_relevant_config(upstream_config)

        self.log.info(f"Local config: {l}")
        self.log.info(f"Upstream config: {u}")

        if l["scratchpad"] != u["scratchpad"] or l["step"] != u["step"]:
            return self.downstream_task_id
        return self.joiner_task_id
