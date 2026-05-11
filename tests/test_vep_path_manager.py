"""Test VEP volume registry options."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from orchestration.models.batch.volume import VolumeRegistrySpec, VolumeSpec
from orchestration.operators.batch.manifest_generators.vep import VepVolumeRegistryOptions


class TestVepVolumeRegistryOptions:
    @pytest.fixture(autouse=True)
    def _setup(self) -> None:
        self.options = VepVolumeRegistryOptions(
            vcf_input_path="gs://bucket/vcf",
            vep_output_path="gs://bucket/output",
            vep_cache_path="gs://bucket/cache",
            mount_dir_root="/mnt/vep/",
        )

    def test_invalid_mount_dir_not_under_mnt(self) -> None:
        """mount_dir_root must start with /mnt."""
        with pytest.raises(ValidationError):
            VepVolumeRegistryOptions(
                vcf_input_path="gs://bucket/vcf",
                vep_output_path="gs://bucket/output",
                vep_cache_path="gs://bucket/cache",
                mount_dir_root="/root/",
            )

    def test_invalid_mount_dir_no_trailing_slash(self) -> None:
        """mount_dir_root must end with a trailing slash."""
        with pytest.raises(ValidationError):
            VepVolumeRegistryOptions(
                vcf_input_path="gs://bucket/vcf",
                vep_output_path="gs://bucket/output",
                vep_cache_path="gs://bucket/cache",
                mount_dir_root="/mnt/vep",
            )

    def test_not_gcs_paths(self) -> None:
        """All path fields must be valid GCS URIs."""
        with pytest.raises(ValidationError):
            VepVolumeRegistryOptions(
                vcf_input_path="vcf_input_path",
                vep_output_path="vep_output_path",
                vep_cache_path="vep_cache_path",
                mount_dir_root="/mnt/vep/",
            )

    def test_vcf_input_property(self) -> None:
        assert self.options.vcf_input == VolumeSpec(
            remote_uri="gs://bucket/vcf",
            mount_point="/mnt/vep/input/",
        )

    def test_vep_output_property(self) -> None:
        assert self.options.vep_output == VolumeSpec(
            remote_uri="gs://bucket/output",
            mount_point="/mnt/vep/output/",
        )

    def test_vep_cache_property(self) -> None:
        assert self.options.vep_cache == VolumeSpec(
            remote_uri="gs://bucket/cache",
            mount_point="/mnt/vep/cache/",
        )

    def test_to_volume_registry(self) -> None:
        assert self.options.to_volume_registry == VolumeRegistrySpec(
            mounting_points=[
                VolumeSpec(remote_uri="gs://bucket/vcf", mount_point="/mnt/vep/input/"),
                VolumeSpec(remote_uri="gs://bucket/output", mount_point="/mnt/vep/output/"),
                VolumeSpec(remote_uri="gs://bucket/cache", mount_point="/mnt/vep/cache/"),
            ]
        )
