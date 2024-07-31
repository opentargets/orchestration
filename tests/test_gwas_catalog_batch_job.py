"""Tests for gwas catalog batch job pipeline."""

from pytest import MonkeyPatch, LogCaptureFixture
from pathlib import Path
from click.testing import CliRunner
from ot_orchestration.cli import cli
import pytest
from ot_orchestration.utils import IOManager
import logging


@pytest.fixture(scope="function")
def staging_path(tmp_path: Path) -> Path:
    """Set the staging path for the integration tests."""
    staging_path = tmp_path / "staging"
    staging_path.mkdir()
    return staging_path


@pytest.fixture(scope="function")
def raw_sumatat_path() -> Path:
    """Raw path for summary statistics dummy test data."""
    return Path(__file__).parent / "data/SSF_GCST001.h.tsv"


@pytest.fixture(scope="function")
def manifest_path(staging_path: Path, raw_sumatat_path: Path) -> str:
    """Dummy manifest saved content from dynamically generated fixtures."""
    output_path = staging_path / "GCST001"
    output_path.mkdir()
    manifest_content = {
        "manifestPath": str(output_path / "manifest.json"),
        "rawPath": str(raw_sumatat_path),
        "harmonisedPath": str(output_path / "harmonised"),
        "qcPath": str(output_path / "qc"),
        "analysisFlag": "Metabolite",
        "isCurated": True,
        "passHarmonisation": True,
        "passQC": False,
        "pubmedId": "28878392",
        "studyId": "GCST001",
        "studyType": "",
    }
    manifest_path = str(output_path / "manifest.json")
    IOManager().resolve(manifest_path).dump(manifest_content)
    return manifest_path


@pytest.mark.integration_test
def test_cli():
    """Assert cli runs without any errors."""
    runner = CliRunner()
    result = runner.invoke(cli)
    print(result.output)
    assert result.exit_code == 0


@pytest.mark.integration_test
def test_gwas_catalog_pipeline_no_env_var(
    monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
):
    """Assert missing MANIFEST_PATH env var produces exit_code = 1"""
    monkeypatch.delenv("MANIFEST_PATH", raising=False)
    runner = CliRunner()
    result = runner.invoke(cli, ["gwas-catalog-pipeline"])
    print(result.output)
    assert result.exit_code == 1
    for record in caplog.records:
        if record.levelname == "ERROR":
            assert "MANIFEST_PATH environment variable is missing" in caplog.text


@pytest.mark.integration_test
@pytest.mark.skip(
    reason="can not install gentropy as hail and apache-beam are incompatible"
)
def test_gwas_catalog_pipeline(
    monkeypatch: MonkeyPatch,
    manifest_path: str,
    caplog: LogCaptureFixture,
):
    """Assert run with MANIFEST_PATH env var successfuly generate harmonisation files."""
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("MANIFEST_PATH", manifest_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["gwas-catalog-pipeline"])
    print(result.exception)
    print(result.output)
    assert result.exit_code == 0
