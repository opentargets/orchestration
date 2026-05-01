"""Tests for PipelineRunConfig."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from orchestration.models.run_config import PipelineRunConfig


# --- valid cases ---

@pytest.mark.parametrize("run_name", [
    pytest.param("sz/platform-2605-1", id="platform current month"),
    pytest.param("sz/ppp-2605-1", id="ppp current month"),
    pytest.param("sz/platform-2606-1", id="future month"),
    pytest.param("sz/platform-2612-99", id="high revision"),
    pytest.param("abc/platform-2605-1", id="three-letter prefix"),
])
def test_valid_run_name(run_name: str) -> None:
    cfg = PipelineRunConfig(run_name=run_name)
    assert cfg.run_name == run_name


# --- invalid format ---

@pytest.mark.parametrize("run_name", [
    pytest.param("sz/Platform-2605-1", id="uppercase flavor"),
    pytest.param("sz/platform-2605", id="missing revision"),
    pytest.param("sz/platform-26051", id="missing dash before revision"),
    pytest.param("platform-2605-1", id="missing personal prefix"),
    pytest.param("sz/unknown-2605-1", id="unknown flavor"),
    pytest.param("sz/platform-2605-1-dev", id="dev suffix not allowed"),
    pytest.param("SZ/platform-2605-1", id="uppercase prefix"),
    pytest.param("s1/platform-2605-1", id="digit in prefix"),
    pytest.param("", id="empty string"),
])
def test_invalid_run_name_format(run_name: str) -> None:
    with pytest.raises(ValidationError, match="run_name"):
        PipelineRunConfig(run_name=run_name)


# --- date guard ---

def test_run_name_past_date_rejected() -> None:
    current_yymm = int(datetime.now().strftime("%y%m"))
    yy = current_yymm // 100
    mm = current_yymm % 100
    if mm == 1:
        past_yymm = f"{yy - 1:02d}12"
    else:
        past_yymm = f"{yy:02d}{mm - 1:02d}"
    run_name = f"sz/platform-{past_yymm}-1"
    with pytest.raises(ValidationError, match="past"):
        PipelineRunConfig(run_name=run_name)


def test_run_name_current_month_accepted() -> None:
    current_yymm = datetime.now().strftime("%y%m")
    cfg = PipelineRunConfig(run_name=f"sz/platform-{current_yymm}-1")
    assert cfg.run_name == f"sz/platform-{current_yymm}-1"


# --- release_uri ---

def test_release_uri_dev() -> None:
    cfg = PipelineRunConfig(run_name="sz/platform-2605-1", is_dev=True)
    assert cfg.release_uri == "gs://open-targets-pipeline-runs/sz/platform-2605-1"


def test_release_uri_prod() -> None:
    cfg = PipelineRunConfig(run_name="sz/platform-2605-1", is_dev=False)
    assert cfg.release_uri == "gs://open-targets-pre-data-releases/platform-2605"


def test_release_uri_default_is_dev() -> None:
    cfg = PipelineRunConfig(run_name="sz/platform-2605-1")
    assert cfg.release_uri == "gs://open-targets-pipeline-runs/sz/platform-2605-1"


# --- release_name ---

def test_release_name_strips_prefix_and_revision() -> None:
    cfg = PipelineRunConfig(run_name="sz/platform-2605-1")
    assert cfg.release_name == "platform-2605"


def test_release_name_ppp() -> None:
    cfg = PipelineRunConfig(run_name="abc/ppp-2605-3")
    assert cfg.release_name == "ppp-2605"
