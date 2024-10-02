"""Test Path utils."""

import re
from pathlib import Path
from typing import Any

import pytest
from ot_orchestration.utils import GCSPath, IOManager
from ot_orchestration.utils.path import (
    URI_PATTERN,
    GCSPath,
    IOManager,
    NativePath,
    extract_partition_from_blob,
)


@pytest.mark.parametrize(
    ["path", "expected_class"],
    [
        pytest.param("some/path", NativePath, id="local path"),
        pytest.param("file://some/path", NativePath, id="fs protocol"),
        pytest.param("gs://some/path", GCSPath, id="gcs protocol"),
    ],
)
def test_io_manager(path: str, expected_class: Any) -> None:
    """Test io manager returns correct Path object after the resolution."""
    assert isinstance(IOManager().resolve(path), expected_class)


@pytest.mark.parametrize(
    ["obj", "suffix"],
    [
        pytest.param("content", "path/file.txt", id="whole subpath does not exist"),
        pytest.param("content", "file.txt", id="file does not exist"),
        pytest.param({"key": "val"}, "tmp.yaml", id="string"),
        pytest.param({"key": "val"}, "tmp.yml", id="string"),
        pytest.param({"key": "val"}, "tmp.json", id="string"),
    ],
)
def test_native_path(tmp_path: Path, suffix: str, obj: Any) -> None:
    """Test NativePath."""
    path = str(tmp_path / suffix)
    native_path = NativePath(path)
    assert not native_path.exists()
    native_path.dump(obj)
    content = native_path.load()
    assert content == obj


@pytest.mark.parametrize(
    ["input_path", "_match", "protocol", "root", "path"],
    [
        pytest.param(
            "gs://bucket_name",
            False,
            None,
            None,
            None,
            id="Regex pattern requires root (bucket) and path (prefix + filename).",
        ),
        pytest.param(
            "gs://root/filename",
            True,
            "gs",
            "root",
            "filename",
            id="Regex pattern for path with protocol, root and filename - without prefix.",
        ),
        pytest.param(
            "gs://root/prefix/filename",
            True,
            "gs",
            "root",
            "prefix/filename",
            id="Regex pattern for path with protocol, root, prefix and filename.",
        ),
    ],
)
def test_uri_pattern_regex(
    input_path: str,
    _match: bool,
    protocol: str | None,
    root: str | None,
    path: str | None,
) -> None:
    """Test URI regex pattern."""
    pattern = re.compile(URI_PATTERN)
    pattern_match = pattern.search(input_path)

    if not _match:
        print(pattern_match)
        assert pattern_match is None
    else:
        assert pattern_match is not None
        assert pattern_match.group("protocol") == protocol
        assert pattern_match.group("root") == root
        assert pattern_match.group("path") == path


class TestGCSPath:
    @pytest.mark.parametrize(
        ["gcs_path"],
        [
            pytest.param("gs://bucket/prefix/filename", id="GCS path with prefix."),
            pytest.param("gs://bucket/filename", id="GCS path without prefix."),
        ],
    )
    def test_reprint(self, gcs_path: str) -> None:
        """Test GCSPath object converts correctly back to string."""
        gcs_path_obj = GCSPath(gcs_path)
        assert str(gcs_path_obj) == gcs_path

    @pytest.mark.parametrize(
        ["gcs_path", "filename", "prefix"],
        [
            pytest.param(
                "gs://bucket/prefix/filename",
                "filename",
                "prefix",
                id="GCS path with prefix.",
            ),
            pytest.param(
                "gs://bucket/filename",
                "filename",
                "",
                id="GCS path without prefix.",
            ),
            pytest.param(
                "gs://bucket/longer/path/filename",
                "filename",
                "longer/path",
                id="GCS path without prefix.",
            ),
        ],
    )
    def test_segments_property(self, gcs_path: str, filename: str, prefix: str) -> None:
        """Test GCSPath object segments property return correct values."""
        gcs_path_obj = GCSPath(gcs_path)
        assert isinstance(gcs_path_obj.segments, dict)
        assert set(gcs_path_obj.segments.keys()) == {
            "protocol",
            "root",
            "prefix",
            "filename",
        }
        print(gcs_path_obj.segments)
        assert gcs_path_obj.segments["protocol"] == "gs"
        assert gcs_path_obj.segments["root"] == "bucket"
        assert gcs_path_obj.segments["prefix"] == prefix
        assert gcs_path_obj.segments["filename"] == filename

    @pytest.mark.parametrize(
        ["gcs_path", "path"],
        [
            pytest.param(
                "gs://bucket/prefix/filename",
                "prefix/filename",
                id="GCS path with prefix.",
            ),
            pytest.param(
                "gs://bucket/filename",
                "filename",
                id="GCS path without prefix.",
            ),
            pytest.param(
                "gs://bucket/longer/path/filename",
                "longer/path/filename",
                id="GCS path without prefix.",
            ),
        ],
    )
    def test_path_property(self, gcs_path: str, path: str) -> None:
        """Test GCSPath object path property."""
        gcs_path_obj = GCSPath(gcs_path)
        assert gcs_path_obj.path == path

    @pytest.mark.parametrize(
        ["gcs_path", "bucket"],
        [
            pytest.param(
                "gs://bucket/prefix/filename",
                "bucket",
                id="GCS path with bucket",
            ),
        ],
    )
    def test_bucket_property(self, gcs_path: str, bucket: str) -> None:
        """Test GCSPath object bucket property."""
        gcs_path_obj = GCSPath(gcs_path)
        assert gcs_path_obj.bucket == bucket


def test_extract_partition_from_blob():
    """Test extract_partition_from_blob."""
    with pytest.raises(ValueError, match="No partition found"):
        extract_partition_from_blob("gs://bucket/prefix/filename")
    # fmt: off
    assert extract_partition_from_blob("gs://bucket/longer/path/partition=123") == "partition=123", "Failed extracting partition"
    assert extract_partition_from_blob("clean_loci.parquet/studyLocusId=999080517901738023") == "studyLocusId=999080517901738023", "Failed extracting Locus partition"
    assert extract_partition_from_blob("clean_loci.parquet/studyLocusId=999080517901738023/partition=123") == "studyLocusId=999080517901738023", "Get the first partition if multiple provided"
    # fmt: on
