"""Unit test configuration for the project."""

from __future__ import annotations

import pytest
from airflow.models import DagBag


@pytest.fixture(params=["orchestration/dags"])
def dag_bag(request: pytest.FixtureRequest) -> DagBag:
    """Return a DAG bag for testing."""
    return DagBag(dag_folder=request.param, include_examples=False)
