"""Custom operators for Google Cloud Storage (GCS) interactions."""

from collections.abc import Sequence
from pathlib import Path

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud.exceptions import NotFound
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
        src_path: The path to the file to upload.
        dst_uri: The destination URI in GCS.
    """

    template_fields: Sequence[str] = ("src", "dst_uri")

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        src_path: Path,
        dst_uri: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.dst_uri = GCSPath(dst_uri)
        self.src_path = src_path

        self.bucket_name, self.path = self.dst_uri.split()

    def execute(self, context) -> None:
        """Execute the Operator."""
        c = Client(project=self.project_id)
        b = Bucket(client=c, name=self.bucket_name)

        if not b.exists():
            b.create()

        blob = b.blob(self.path)
        blob.upload_from_filename(self.src_path)
        self.log.info("uploaded file from %s to: %s", self.src_path, self.dst_uri)


class UploadRemoteFileOperator(BaseOperator):
    """Custom operator that uploads a remote file to GCS.

    This operator will create a GCS bucket if it does not exist and upload the
    file from a URL to a path inside that bucket.

    Args:
        project_id: The GCP project ID. Defaults to the platform project.
        src_url: Source file URL.
        dst_uri: The destination URI in GCS.
    """

    template_fields: Sequence[str] = ("src_url", "dst_uri")

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        src_url: str,
        dst_uri: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.src_url = src_url
        self.dst_uri = GCSPath(dst_uri)

        self.bucket_name, self.path = self.dst_uri.split()

    def execute(self, context) -> None:
        """Execute the Operator."""
        c = Client(project=self.project_id)
        b = Bucket(client=c, name=self.bucket_name)
        temp_file = Path("/tmp") / self.src_url.split("/")[-1]

        with requests.get(self.src_url, stream=True) as r:
            r.raise_for_status()
            with open(temp_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        if not b.exists():
            b.create()

        blob = b.blob(self.path)
        blob.upload_from_filename(temp_file)
        self.log.info("uploaded file from %s to: %s", self.src_url, self.dst_uri)


class UploadStringOperator(BaseOperator):
    """Custom operator that uploads a string to GCS.

    This operator will create a GCS bucket if it does not exist and upload the
    given string to the specified path inside that bucket.

    An error will be raised if a file with the destination name already exists
    and overwrite is not set to `True`.

    Args:
        project_id: The GCP project ID. Defaults to the platform project.
        contents: The string to upload.
        dst_uri: The destination URI in GCS.
    """

    template_fields: Sequence[str] = ("contents", "dst_uri")

    def __init__(
        self,
        *args,
        project_id: str = GCP_PROJECT_PLATFORM,
        contents: str,
        dst_uri: str,
        overwrite: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.dst_uri = GCSPath(dst_uri)
        self.contents = contents
        self.overwrite = overwrite
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.bucket_name, self.path = self.dst_uri.split()

    def execute(self, context) -> None:
        """Execute the Operator."""
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        c = hook.get_conn()
        try:
            c.get_bucket(self.bucket_name)
        except NotFound:
            hook.create_bucket(bucket_name=self.bucket_name)

        if not self.overwrite and hook.exists(self.bucket_name, self.path):
            raise FileExistsError(f"Destination object {self.dst_uri} already exists.")

        hook.upload(
            bucket_name=self.bucket_name,
            object_name=self.path,
            data=self.contents,
        )
        self.log.info("uploaded string to: %s", self.dst_uri)


class CopyBlobOperator(BaseOperator):
    """Custom operator that copies a GCS blob to another location.

    The operator will make sure the source blob exists and will raise an error
    otherwise. Regarding the destination file, an error will be raised if it
    already exists and overwrite is not set to `True`.

    Args:
        src_uri: The source GCS URI.
        dst_uri: The destination GCS URI.
        overwrite: Whether to overwrite the destination file if it already exists.
        gcp_conn_id: The connection ID to use when connecting to GCS.
        impersonation_chain: The service account to impersonate.
    """

    template_fields: Sequence[str] = ("src_uri", "dst_uri", "impersonation_chain")

    def __init__(
        self,
        *,
        src_uri: str,
        dst_uri: str,
        overwrite: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.src_uri = src_uri
        self.dst_uri = dst_uri
        self.overwrite = overwrite
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        super().__init__(**kwargs)

    def execute(self, context) -> None:
        """Execute the Operator."""
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        source_bucket, source_object = self.src_uri.replace("gs://", "").split("/", 1)
        destination_bucket, destination_object = self.dst_uri.replace("gs://", "").split("/", 1)

        if not hook.exists(source_bucket, source_object):
            raise FileNotFoundError(f"Source object {self.src_uri} does not exist.")
        if not self.overwrite and hook.exists(destination_bucket, destination_object):
            raise FileExistsError(f"Destination object {self.dst_uri} already exists.")

        self.log.info("copying %s to %s", self.src_uri, self.dst_uri)
        hook.copy(source_bucket, source_object, destination_bucket, destination_object)
