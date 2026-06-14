"""Tests for PipelineRunConfig."""

import datetime

import pytest
from pydantic import ValidationError

from orchestration.models.run_config import PipelineRunConfig

# Use current month for all tests since date guard was removed.


@pytest.fixture
def current_yymm() -> str:
    return datetime.datetime.now(datetime.UTC).strftime("%y%m")


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


def test_release_uri_dev(current_yymm: str) -> None:
    """release_uri returns the dev bucket path when is_dev=True."""
    cfg = PipelineRunConfig(run_name=f"sz/platform-{current_yymm}-1", is_dev=True)
    assert cfg.release_uri == f"gs://open-targets-pipeline-runs/sz/platform-{current_yymm}-1"


def test_release_uri_prod(current_yymm: str) -> None:
    """release_uri returns the release bucket path when is_dev=False."""
    cfg = PipelineRunConfig(run_name=f"sz/platform-{current_yymm}-1", is_dev=False)
    assert cfg.release_uri == f"gs://open-targets-pre-data-releases/platform-{current_yymm}"


def test_release_uri_default_is_dev(current_yymm: str) -> None:
    """release_uri defaults to the dev bucket path."""
    cfg = PipelineRunConfig(run_name=f"sz/platform-{current_yymm}-1")
    assert cfg.release_uri == f"gs://open-targets-pipeline-runs/sz/platform-{current_yymm}-1"


# ---------------------------------------------------------------------------
# release_name
# ---------------------------------------------------------------------------


def test_release_name_strips_prefix_and_revision(current_yymm: str) -> None:
    """release_name strips the personal prefix and revision number."""
    cfg = PipelineRunConfig(run_name=f"sz/platform-{current_yymm}-1")
    assert cfg.release_name == f"platform-{current_yymm}"


def test_release_name_ppp(current_yymm: str) -> None:
    """release_name works correctly for ppp flavor."""
    cfg = PipelineRunConfig(run_name=f"abc/ppp-{current_yymm}-3")
    assert cfg.release_name == f"ppp-{current_yymm}"
