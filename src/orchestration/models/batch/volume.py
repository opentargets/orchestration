"""Volume specifications for Google Batch tasks."""

import logging
from functools import cached_property
from typing import Annotated

from google.cloud import batch_v1
from pydantic import BaseModel, Field

from orchestration.utils.path import GCSPath

logger = logging.getLogger(__name__)


class VolumeSpec(BaseModel):
    """Volume specification for Google Batch tasks."""

    remote_uri: Annotated[str, Field(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*/?$")]
    """GCS path to be mounted."""
    mount_point: Annotated[str, Field(pattern=r"^/mnt(/[a-zA-Z0-9_-]+)*/?$")]
    """Local path on the google batch VM where the GCS path will be mounted."""
    mount_options: list[str] = []
    """Extra gcsfuse options passed to batch_v1.Volume.mount_options (e.g. ['--billing-project=my-project'])."""

    @cached_property
    def gcs_path(self) -> GCSPath:
        """Return the GCS path.

        The GCS path is derived from the remote URI by stripping the "gs://" prefix and any
        trailing slashes.

        Returns:
            GCSPath: The GCS path.

        """
        return GCSPath(self.remote_uri.rstrip("/"))

    @cached_property
    def remote_path(self) -> str:
        """Return the remote path.

        The remote path is derived from the remote URI by stripping the "gs://" prefix and any
        trailing slashes.

        Returns:
            str: The remote path.

        Example:
        ---
        >>> VolumeSpec(remote_uri="gs://my-bucket/my/path", mount_point="/mnt/data/").remote_path
        'my-bucket/my/path'
        """
        gcs_path = self.gcs_path
        return f"{gcs_path.bucket}/{gcs_path.path}"


class VolumeRegistrySpec(BaseModel):
    """Volume registry for Google Batch tasks."""

    mounting_points: list[VolumeSpec]
    """List of volume specifications for Google Batch tasks."""

    def build(self) -> list[batch_v1.Volume]:
        """Set up the mounting points for the container.

        Returns:
            list[batch_v1.Volume]: The volumes.

        Example:
        ---
        >>> reg = VolumeRegistrySpec(mounting_points=[
        ...     VolumeSpec(remote_uri="gs://bucket-a/data", mount_point="/mnt/a/"),
        ...     VolumeSpec(remote_uri="gs://bucket-b/cache", mount_point="/mnt/b/"),
        ... ])
        >>> vols = reg.build()
        >>> len(vols)
        2
        >>> all(isinstance(v, batch_v1.Volume) for v in vols)
        True
        >>> vols[0].gcs.remote_path
        'bucket-a/data'
        >>> vols[0].mount_path
        '/mnt/a'
        """
        volumes = []
        for mount in self.mounting_points:
            # Google Batch does not allow trailing slashes in the mount points, so we need to strip them.
            mount_point_safe = mount.mount_point.rstrip("/")
            gcs_object = batch_v1.GCS(remote_path=mount.remote_path)
            gcs_volume = batch_v1.Volume(gcs=gcs_object, mount_path=mount_point_safe, mount_options=mount.mount_options)
            logger.debug("Built volume with remote path %s and mount point %s", mount.remote_path, mount_point_safe)
            volumes.append(gcs_volume)
        return volumes
