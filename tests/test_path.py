"""Test Path utils."""

import pytest
from ot_orchestration.utils.path import IOManager, NativePath, GCSPath
from pathlib import Path
from typing import Any
from pytest import MonkeyPatch


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
    ["obj", "file"],
    [
        pytest.param("some_content", "tmp.txt", id="text io wrapper"),
        pytest.param({"key": "val"}, "tmp.yaml", id="string"),
        pytest.param({"key": "val"}, "tmp.yml", id="string"),
        pytest.param({"key": "val"}, "tmp.json", id="string"),
    ],
)
def test_gcs_path(
    tmp_path: Path, monkeypatch: MonkeyPatch, obj: Any, file: str
) -> None:
    """Test GCSPath."""
    tmp_file = tmp_path / file

    def mock_open(_, mode, *args, **kwargs):
        """Mock open."""
        return open(tmp_file, mode)

    monkeypatch.setattr("google.cloud.storage.blob.Blob.open", mock_open)
    path = "gs://" + str(tmp_file)
    gcs_path = GCSPath(path)
    gcs_path.dump(obj)
    content = gcs_path.load()
    assert content == obj
