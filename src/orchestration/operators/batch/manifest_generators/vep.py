"""Vep manifest generator."""

from __future__ import annotations

from logging import getLogger
from typing import Annotated

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pydantic import BaseModel, Field

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.models.batch.environment import EnvironmentRegistrySpec, EnvironmentSpec
from orchestration.models.batch.volume import VolumeRegistrySpec, VolumeSpec
from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators.proto import ProtoManifestGenerator
from orchestration.utils.common import GCP_PROJECT_PLATFORM
from orchestration.utils.path import GCSPath

logger = getLogger(__name__)


class VepVolumeRegistryOptions(BaseModel):
    """Variant Effect Predictor (VEP) path mount configuration.

    This class represents the configuration for mounting the necessary paths
    for the VEP annotation step running on google batch VMs.

    Attributes:
        vcf_input_path (str): GCS path that contains all input VCF files.
        vep_output_path (str): GCS path where the output of the VEP annotation should be stored.
        vep_cache_path (str): GCS path to the VEP cache
        mount_dir_root (str): Mount directory root for Vep google batch tasks. This should be an absolute path. The default value is /mnt/vep


    The configuration contains a single method `to_path_registry` that converts the **path configuration into a path registry**.

    The path registry contains 3 keys `input`, `output` and `cache` that correspond to the input, output and cache paths respectively.
    The value of each key is a dictionary that contains the `remote_path` (GCS path) and the `mount_point` (local path on the google batch VM).

    The mount points are derived from the `mount_dir_root` attribute and the path keys.
    """

    vcf_input_path: Annotated[str, Field(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*/?$")]
    """GCS path that contains all input VCF files."""
    vep_output_path: Annotated[str, Field(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*/?$")]
    """GCS path where the output of the VEP annotation should be stored."""
    vep_cache_path: Annotated[str, Field(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*/?$")]
    """GCS path to the VEP cache."""
    mount_dir_root: Annotated[str, Field(pattern=r"^/mnt(/[a-zA-Z0-9_-]+)*/$")] = "/mnt/disks/share/"
    """Mount directory root for Vep google batch tasks. This should be an absolute path. The default value is /mnt/disks/share/."""

    @property
    def vcf_input(self) -> VolumeSpec:
        """Get vcf input path."""
        return VolumeSpec(remote_uri=self.vcf_input_path.rstrip("/"), mount_point=f"{self.mount_dir_root}input/")

    @property
    def vep_output(self) -> VolumeSpec:
        """Get vep output path."""
        return VolumeSpec(remote_uri=self.vep_output_path.rstrip("/"), mount_point=f"{self.mount_dir_root}output/")

    @property
    def vep_cache(self) -> VolumeSpec:
        """Get vep cache path."""
        return VolumeSpec(remote_uri=self.vep_cache_path.rstrip("/"), mount_point=f"{self.mount_dir_root}cache/")

    @property
    def to_volume_registry(self) -> VolumeRegistrySpec:
        """Get all paths."""
        return VolumeRegistrySpec(
            mounting_points=[
                self.vcf_input,
                self.vep_output,
                self.vep_cache,
            ],
        )


class VepManifestGenerator(ProtoManifestGenerator):
    """Manifest generator for VEP annotation step running on google batch."""

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> VepManifestGenerator:
        """Build Generator from generator specs."""
        return cls(
            options=VepVolumeRegistryOptions(**specs.generator_options),
        )

    def __init__(
        self,
        *,
        options: VepVolumeRegistryOptions,
        gcp_conn_id: str = "google_cloud_default",
        project_id: str = GCP_PROJECT_PLATFORM,
    ) -> None:
        """Initialize the manifest generator.

        Args:
            options (VepVolumeRegistryOptions): Options for the VEP volume registry.
            gcp_conn_id (str, optional): Google cloud connection. Defaults to "google_cloud_default".
            project_id (str, optional): Google cloud project id. Defaults to GCP_PROJECT_PLATFORM.
        The `options` represent the way to partition the input dataset.

        The default value provided should follow the VepVolumeConfiguration schema.
        Depending on the number of files that match the `vcf_input_path`, the generator will create a `BatchIndex`
        of these files with the corresponding mount configuration for each of them.


        """
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.options = options
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)

    def generate_batch_index(self) -> BatchIndex:
        """Generate index for google batch tasks."""
        return BatchIndex(env_registry=self._build_environment_registry())

    def _get_vcf_partition_basenames(self, input_path: GCSPath) -> set[str]:
        """Based on listed vcf file partition extract their base names.

        NOTE: Do not reconstruct full path to the mount, as it will
        reduce the payload send to the google batch job. The mount
        name is the same at every task command, the basename is
        different.

        Returns:
            set[str]: set of base names to pass to the task environments.
        """
        blobs = self.gcs_hook.list(input_path.bucket, prefix=input_path.path, match_glob="**.csv")
        vcf_paths = {GCSPath(f"gs://{input_path.bucket}/{blob}").segments["filename"] for blob in blobs}
        logger.info("Found %s vcf files", len(vcf_paths))
        return vcf_paths

    def _build_environment_registry(self) -> EnvironmentRegistrySpec:
        """Build the list of variables to be used in the manifest.

        The list is built by listing the number of VCF files in the `vcf_input_path` and creating a dictionary for each file with the corresponding mount configuration.

        Returns:
            EnvironmentRegistrySpec: List of variables to be used in the manifest.
        """
        vcf_files = self._get_vcf_partition_basenames(self.options.vcf_input.gcs_path)
        # The content looks like:
        # [{"INPUT_FILE": "file1.vcf", "OUTPUT_FILE": "file1.json"}, {"INPUT_FILE": "file2.vcf", "OUTPUT_FILE": "file2.json"}, ...]
        return EnvironmentRegistrySpec(
            environments=[
                EnvironmentSpec(
                    variables={
                        "INPUT_FILE": file,
                        "OUTPUT_FILE": file.replace(".csv", ".json"),
                    }
                )
                for file in vcf_files
            ]
        )
