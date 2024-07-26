"""Custom operator that uploads configurations to GCS.

This operator will create a GCS bucket if it does not exist and upload the
configurations to the specified path inside that bucket.
"""

from pathlib import Path

from airflow.models.baseoperator import BaseOperator
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from ot_orchestration.utils.utils import bucket_name, bucket_path


class UploadConfigOperator(BaseOperator):
    """Custom operator that uploads configurations to GCS."""

    template_fields = ["src", "gcs_url"]

    def __init__(
        self,
        *args,
        project_id: str,
        gcs_url: str,
        src: Path,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        # try to get upload url from yaml_file
        self.project_id = project_id
        self.gcs_url = gcs_url
        self.src = src

        self.bucket_name = bucket_name(self.gcs_url)
        self.path = bucket_path(self.gcs_url)

    def execute(self, context) -> None:
        """Upload the configurations to GCS."""
        c = Client(project=self.project_id)
        b = Bucket(client=c, name=self.bucket_name)

        if not b.exists():
            b.create()

        blob = b.blob(self.path)
        blob.upload_from_filename(self.src)
