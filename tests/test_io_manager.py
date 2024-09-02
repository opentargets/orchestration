"""Test Path utils."""

import re
from pathlib import Path
from typing import Any

import pytest
from ot_orchestration.utils import GCSPath, IOManager
from ot_orchestration.utils.path import POSIX_PATH_PATTERN, IOManager, NativePath


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
    ["path", "_match", "protocol", "root", "prefix", "filename"],
    [
        pytest.param(
            "gs://filename",
            False,
            None,
            None,
            None,
            None,
            id="Regex pattern requires root (bucket) and prefix.",
        ),
        pytest.param(
            "gs://root/filename",
            False,
            None,
            None,
            None,
            None,
            id="Regex pattern requires prefix.",
        ),
        pytest.param(
            "gs://root/prefix/filename",
            True,
            "gs",
            "root",
            "prefix",
            "filename",
            id="Regex pattern for path with protocol, root, prefix and filename.",
        ),
    ],
)
def test_posix_path_regex(
    path: str,
    _match: bool,
    protocol: str | None,
    root: str | None,
    prefix: str | None,
    filename: str | None,
) -> None:
    """Test POSIX path regex."""
    pattern = re.compile(POSIX_PATH_PATTERN)
    pattern_match = pattern.search(path)

    if not _match:
        print(pattern_match)
        assert pattern_match is None
    else:
        assert pattern_match is not None
        assert pattern_match.group("protocol") == protocol
        assert pattern_match.group("root") == root
        assert pattern_match.group("prefix") == prefix
        assert pattern_match.group("filename") == filename
