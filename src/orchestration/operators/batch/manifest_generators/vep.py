"""Vep manifest generator."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path

from orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from orchestration.types import GCSMountObject, GoogleBatchSpecs, ManifestGeneratorSpecs
from orchestration.utils.path import GCSPath


class VepAnnotationPathManager:
    """Manager class for setting correct mounting points for VEP google batch tasks."""

    def __init__(
        self,
        vcf_input_path: str,
        vep_output_path: str,
        vep_cache_path: str,
        mount_dir_root: str,
    ):
        self._mount_dir_root = mount_dir_root
        self.paths = {
            "input": GCSPath(vcf_input_path),
            "output": GCSPath(vep_output_path),
            "cache": GCSPath(vep_cache_path),
        }

    @cached_property
    def mount_dir_root(self) -> str:
        """Get the mount directory root."""
        if not self._mount_dir_root.startswith("/"):
            raise ValueError("Mount dir has to be an absolute path.")
        if self._mount_dir_root.endswith("/"):
            return str(Path(self._mount_dir_root))
        return self._mount_dir_root

    @cached_property
    def path_registry(self) -> dict[str, GCSMountObject]:
        """Get the path registry."""
        return {
            key: {
                # NOTE: remote_path has to start from the bucket_name but without the gs://
                # see https://cloud.google.com/batch/docs/create-run-job-storage#gcloud_2:~:text=BUCKET_PATH%3A%20the%20path,the%20subdirectory%20subdirectory.
                "remote_path": f"{value.bucket}/{value.path}",
                "mount_point": f"{self.mount_dir_root}/{key}",
            }
            for key, value in self.paths.items()
        }

    @cached_property
    def cache_dir(self) -> str:
        """Get cache dir."""
        return self.path_registry["cache"]["mount_point"]

    @cached_property
    def input_dir(self) -> str:
        """Get input dir."""
        return self.path_registry["input"]["mount_point"]

    @cached_property
    def output_dir(self) -> str:
        """Get output dir."""
        return self.path_registry["output"]["mount_point"]

    @cached_property
    def mount_config(self) -> list[GCSMountObject]:
        """Return the mount configuration.

        Returns:
            list[dict[str, str]]: The mount configuration.
        """
        return list(self.path_registry.values())


class VepManifestGenerator(ProtoManifestGenerator):
    """Manifest generator for VEP annotation step running on google batch."""

    def __init__(
        self,
        *,
        commands: list[str],
        options: dict[str, str],
        manifest_kwargs: dict[str, str],
        gcp_conn_id: str = "google_cloud_default",
    ) -> None:
        """Initialize the manifest generator.

        Args:
            commands (list[str]): List of commands to run the VEP annotation step.
            options (dict[str, str]): dictionary of options to run the step. Typically these are {"step": "vep_annotation"}.
            manifest_kwargs (dict[str, str]): Arguments used to derive the batch job partitioning.
            gcp_conn_id (str, optional): Google cloud connection. Defaults to "google_cloud_default".

        The `manifest_kwargs` represent the way to partition the input dataset. The default value provided should be
        {"vcf_input_path": "gs://bucket_name/some/prefix/**.vcf", "vep_output_path": "gs://bucket_name/some/output/prefix", "vep_cache_path": "gs://bucket_name/some/vep/cache/path", "mount_dir_root": "/mnt/vep"}.
        Depending on the number of files that match the `vcf_input_path` glob pattern the computed google batch job definition will have corresponding number of tasks.
        """
        self.commands = commands
        self.options = options

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpecs) -> VepManifestGenerator:
        """Build Generator from generator specs."""
        return cls(
            commands=specs["commands"],
            options=specs["options"],
            manifest_kwargs=specs["manifest_kwargs"],
        )

    def generate_batch_index(self) -> BatchIndex:
        """Generate index for google batch tasks."""
        vars_list = self.build_vars_list()
        return BatchIndex(
            vars_list=vars_list,
            options=self.options,
            commands=self.commands,
        )
