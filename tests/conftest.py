"""Unit test configuration for the project."""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag


@pytest.fixture(params=["orchestration/dags"])
def dag_bag(request: pytest.FixtureRequest) -> DagBag:
    """Return a DAG bag for testing."""
    return DagBag(dag_folder=request.param, include_examples=False)


@pytest.fixture
def config_dir() -> Path:
    """Return the path to the config directory."""
    c = Path(__file__).parent.parent / "src" / "orchestration" / "dags" / "config"
    assert c.is_dir(), f"Config directory {c} does not exist"
    return c
