"""Custom operators for Google Cloud Storage (GCS) interactions."""

from collections.abc import Sequence
from pathlib import Path

import requests
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
        self.project_id = project_id
        self.dst = GCSPath(dst)
        self.src = src

        self.bucket_name, self.path = self.dst.split()

    def execute(self, context) -> None:
        """Execute the Operator."""
        c = Client(project=self.project_id)
        b = Bucket(client=c, name=self.bucket_name)

        if not b.exists():
            b.create()

        blob = b.blob(self.path)
        blob.upload_from_filename(self.src)
        self.log.info("uploaded file from %s to: %s", self.src, self.dst)


class UploadRemoteFileOperator(BaseOperator):
    """Custom operator that uploads a remote file to GCS.

    This operator will create a GCS bucket if it does not exist and upload the
    file from a URL to a path inside that bucket.

    Args:
        project_id: The GCP project ID. Defaults to the platform project.
        src: The URL to the file to upload.
        dst: The destination path in GCS.
    """

    template_fields: Sequence[str] = ("src", "dst")

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        src: str,
        dst: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.dst = GCSPath(dst)
        self.src = src

        self.bucket_name, self.path = self.dst.split()

    def execute(self, context) -> None:
        """Execute the Operator."""
        c = Client(project=self.project_id)
        b = Bucket(client=c, name=self.bucket_name)
        temp_file = Path("/tmp") / self.src.split("/")[-1]

        with requests.get(self.src, stream=True) as r:
            r.raise_for_status()
            with open(temp_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        if not b.exists():
            b.create()

        blob = b.blob(self.path)
        blob.upload_from_filename(temp_file)
        self.log.info("uploaded file from %s to: %s", self.src, self.dst)


class UploadStringOperator(BaseOperator):
    """Custom operator that uploads a string to GCS.

    This operator will create a GCS bucket if it does not exist and upload the
    given string to the specified path inside that bucket.

    Args:
        project_id: The GCP project ID. Defaults to the platform project.
        contents: The string to upload.
        dst: The destination path in GCS.
    """

    template_fields: Sequence[str] = ("contents", "dst")

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        contents: str,
        dst: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.dst = GCSPath(dst)
        self.contents = contents

        self.bucket_name, self.path = self.dst.split()

    def execute(self, context) -> None:
        """Execute the Operator."""
        c = Client(project=self.project_id)
        b = Bucket(client=c, name=self.bucket_name)

        if not b.exists():
            b.create()

        blob = b.blob(self.path)
        blob.upload_from_string(data=self.contents, content_type="text/plain")
        self.log.info("uploaded string to: %s", self.dst)
