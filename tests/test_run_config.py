"""Tests for PipelineRunConfig."""

import pytest
from pydantic import ValidationError

from orchestration.models.run_config import PipelineRunConfig


# ---------------------------------------------------------------------------
# Valid run_name cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("run_name", [
    pytest.param("sz/platform-2605-1", id="platform format"),
    pytest.param("sz/ppp-2605-1", id="ppp format"),
    pytest.param("pt01/platform-2605-1", id="prefix with digits"),
    pytest.param("abc/ppp-2606-2", id="three-letter prefix, high revision"),
])
def test_valid_run_name(run_name: str) -> None:
    """Valid run_name values are accepted by PipelineRunConfig."""
    cfg = PipelineRunConfig(run_name=run_name)
    assert cfg.run_name == run_name


# ---------------------------------------------------------------------------
# Invalid format cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("run_name", [
    pytest.param("sz/Platform-2605-1", id="uppercase flavor"),
    pytest.param("sz/platform-2605", id="missing revision"),
    pytest.param("sz/platform-26051", id="missing dash before revision"),
    pytest.param("platform-2605-1", id="missing personal prefix"),
    pytest.param("sz/unknown-2605-1", id="unknown flavor"),
    pytest.param("sz/platform-2605-1-dev", id="dev suffix not allowed"),
    pytest.param("SZ/platform-2605-1", id="uppercase prefix"),
    pytest.param("1s/platform-2605-1", id="digit at start of prefix"),
    pytest.param("", id="empty string"),
])
def test_invalid_run_name_format(run_name: str) -> None:
    """Malformed run_name values raise ValidationError."""
    with pytest.raises(ValidationError, match="run_name"):
        PipelineRunConfig(run_name=run_name)


@pytest.mark.parametrize("run_name", [
    pytest.param("sz/platform-2605-0", id="zero revision"),
    pytest.param("sz/platform-2605-00", id="double zero revision"),
    pytest.param("sz/platform-2605-000", id="triple zero revision"),
])
def test_invalid_zero_revision(run_name: str) -> None:
    """Revision zero is rejected even when zero-padded."""
    with pytest.raises(ValidationError, match="revision"):
        PipelineRunConfig(run_name=run_name)


# ---------------------------------------------------------------------------
# is_ppp derivation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("run_name", "expected"), [
    ("sz/platform-2605-1", False),
    ("abc/ppp-2606-1", True),
    ("pt01/ppp-9901-5", True),
    ("s/platform-0012-99", False),
])
def test_is_ppp_derived_correctly(run_name: str, expected: bool) -> None:
    """is_ppp is derived from the flavor portion of run_name."""
    cfg = PipelineRunConfig(run_name=run_name)
    assert cfg.is_ppp is expected


# ---------------------------------------------------------------------------
# release_uri
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("is_dev", [
    pytest.param(True, id="legacy true"),
    pytest.param(False, id="legacy false"),
])
def test_legacy_is_dev_input_is_rejected(is_dev: bool) -> None:
    """Legacy is_dev input is rejected during validation."""
    with pytest.raises(ValidationError) as excinfo:
        PipelineRunConfig.model_validate({"run_name": "sz/platform-2605-1", "is_dev": is_dev})

    is_dev_errors = [error for error in excinfo.value.errors() if error.get("loc") == ("is_dev",)]

    assert is_dev_errors
    assert any(
        error.get("type") == "extra_forbidden"
        or any(term in error.get("msg", "").lower() for term in ("unsupported", "forbidden", "not permitted"))
        for error in is_dev_errors
    )


def test_release_uri_uses_pipeline_runs_bucket() -> None:
    """release_uri always points at the pipeline-runs bucket."""
    cfg = PipelineRunConfig(run_name="sz/platform-2605-1")
    assert cfg.release_uri == "gs://open-targets-pipeline-runs/sz/platform-2605-1"


# ---------------------------------------------------------------------------
# release_name
# ---------------------------------------------------------------------------


def test_release_name_strips_prefix_and_revision() -> None:
    """release_name strips the personal prefix and revision number."""
    cfg = PipelineRunConfig(run_name="sz/platform-2605-1")
    assert cfg.release_name == "platform-2605"


def test_release_name_ppp() -> None:
    """release_name works correctly for ppp flavor."""
    cfg = PipelineRunConfig(run_name="abc/ppp-2606-3")
    assert cfg.release_name == "ppp-2606"
