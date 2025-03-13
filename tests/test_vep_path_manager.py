"""Test vep manager."""

from __future__ import annotations

import pytest

from orchestration.operators.batch.vep import VepAnnotationPathManager


class TestVepAnnotationPathManger:
    @pytest.fixture(autouse=True)
    def _setup(self) -> TestVepAnnotationPathManger:
        """Setup vep annotation paths."""
        vep_cache_path = "gs://bucket/cache"
        mount_dir_root = "/root"
        vcf_input_path = "gs://bucket/vcf"
        vep_output_path = "gs://bucket/output"

        self.vp = VepAnnotationPathManager(
            vcf_input_path=vcf_input_path,
            vep_output_path=vep_output_path,
            vep_cache_path=vep_cache_path,
            mount_dir_root=mount_dir_root,
        )
        return self

    def test_invalid_mount_dir(self) -> None:
        """Mount dir must be an absolute path."""
        vp = VepAnnotationPathManager(
            "vcf_input_path",
            "vep_output_path",
            "vep_cache_path",
            "root",
        )
        with pytest.raises(ValueError) as e:
            vp.mount_dir_root
            assert e.value.args[0] == "Mount dir has to be an absolute path."

    def test_mount_dir_ends_with_slash(self) -> None:
        """Assert that mount dir trailing slash will be dropped."""
        vp = VepAnnotationPathManager(
            "vcf_input_path",
            "vep_output_path",
            "vep_cache_path",
            "/root//",
        )

        assert vp.mount_dir_root == "/root"

    def test_not_gcs_paths(self) -> None:
        """Mount dir must be an absolute path."""
        vp = VepAnnotationPathManager(
            "vcf_input_path",
            "vep_output_path",
            "vep_cache_path",
            "/root",
        )
        with pytest.raises(ValueError) as e:
            vp.path_registry
            assert "Invalid GCS path" in e.value.args[0]

    def test_mount_dir_property(self) -> None:
        """Test correct paths provided."""
        assert self.vp.mount_dir_root == "/root"

    def test_cache_dir_property(self) -> None:
        """Test if the `cache_dir property exists and returns correct value."""
        assert self.vp.cache_dir == "/root/cache"

    def test_output_dir_property(self) -> None:
        """Test if the `output_dir` property exists and returns correct value."""
        assert self.vp.output_dir == "/root/output"

    def test_input_dir_property(self) -> None:
        """Test if the `input_dir` property exists and returns correct value."""
        assert self.vp.input_dir == "/root/input"

    def test_path_registry_property(self) -> None:
        """Test if the `path_registry` property returns correct value."""
        assert self.vp.path_registry["input"] == {
            "remote_path": "bucket/vcf",
            "mount_point": "/root/input",
        }
        assert self.vp.path_registry["output"] == {
            "remote_path": "bucket/output",
            "mount_point": "/root/output",
        }
        assert self.vp.path_registry["cache"] == {
            "remote_path": "bucket/cache",
            "mount_point": "/root/cache",
        }

    def test_mount_config(self) -> None:
        """Test if the `mount_config` property returns correct value."""
        assert self.vp.mount_config == [
            {
                "remote_path": "bucket/vcf",
                "mount_point": "/root/input",
            },
            {
                "remote_path": "bucket/output",
                "mount_point": "/root/output",
            },
            {
                "remote_path": "bucket/cache",
                "mount_point": "/root/cache",
            },
        ]
