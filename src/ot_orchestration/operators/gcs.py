"""Custom operators for Google Cloud Storage (GCS) interactions."""

from collections.abc import Sequence
from pathlib import Path

from airflow.models.baseoperator import BaseOperator
from google.cloud.storage import Client
from google.cloud.storage.bucket import Bucket

from ot_orchestration.utils import GCSPath
from ot_orchestration.utils.common import GCP_PROJECT_PLATFORM


class UploadFileOperator(BaseOperator):
    """Custom operator that uploads a file to GCS.

    This operator will create a GCS bucket if it does not exist and upload the
    file to the specified path inside that bucket.

    Args:
        project_id: The GCP project ID. Defaults to the platform project.
        src: The path to the file to upload.
        dst: The destination path in GCS.
    """

    template_fields: Sequence[str] = ("src", "dst")

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        src: Path,
        dst: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        # try to get upload url from yaml_file
        self.project_id = project_id
        self.dst = GCSPath(dst)
        self.src = src

        self.bucket_name, self.path = self.dst.split()

    def execute(self, context) -> None:
        """Upload the configurations to GCS."""
        c = Client(project=self.project_id)
        b = Bucket(client=c, name=self.bucket_name)

        if not b.exists():
            b.create()

        blob = b.blob(self.path)
        blob.upload_from_filename(self.src)
