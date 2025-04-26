"""Tests for package util functions."""

import pytest

from orchestration.types import ConfigNode
from orchestration.utils import convert_params_to_hydra_positional_arg, find_node_in_config, time_to_seconds


@pytest.mark.parametrize(
    ("input", "output"),
    [
        pytest.param("1s", 1, id="1 second"),
        pytest.param("10m", 60 * 10, id="10 minutes"),
        pytest.param("1h", 60 * 60, id="1 hour"),
        pytest.param("1d", 60 * 60 * 24, id="1 day"),
    ],
)
def test_time_to_seconds(input: str, output: int) -> None:
    """Test different date intervals to seconds."""
    assert time_to_seconds(input) == output


@pytest.mark.parametrize(
    ("input", "output", "is_dataproc_job"),
    [
        pytest.param(
            {"step": "some_step", "step.b": 2, "+step.c": 3},
            ["step=some_step", "step.b=2", "+step.c=3"],
            False,
            id="step configuration",
        ),
        pytest.param(
            {"step": "some_step", "step.b": {"c": 2, "d": 3}},
            ["step=some_step", "step.b.c=2", "step.b.d=3"],
            False,
            id="nested dict",
            marks=pytest.mark.xfail(reason="Structured configuration not supported yet."),
        ),
        pytest.param(
            {"step": "some_step", "step.b": 2, "+step.c": 3},
            ["step=some_step", "step.b=2", "+step.c=3", "step.session.spark_uri=yarn"],
            True,
            id="Running with dataproc=True adds yarn as a parameter",
        ),
        pytest.param(
            {
                "step": "some_step",
                "step.b": 2,
                "+step.c": 3,
                "step.session.spark_uri": "yarn",
            },
            ["step=some_step", "step.b=2", "+step.c=3", "step.session.spark_uri=yarn"],
            True,
            id="Running with dataproc=True and yarn present does not duplicate parameter",
        ),
    ],
)
def test_convert_params_to_hydra_positional_arg(input: dict, output: list[str], is_dataproc_job: bool) -> None:
    """Test conversion of dictionary to hydra positional arguments."""
    assert convert_params_to_hydra_positional_arg(input, is_dataproc_job) == output


@pytest.mark.parametrize(
    ("node", "result"),
    [
        pytest.param("A", {"id": "A", "kind": "Task", "prerequisites": []}, id="Existing node"),
        pytest.param("D", None, id="Non existing node"),
    ],
)
def test_find_node_in_config(node: str, result: ConfigNode | None) -> None:
    """Test finding a node in a configuration."""
    config_list = [
        {"id": "A", "kind": "Task", "prerequisites": []},
        {"id": "B", "kind": "Task", "prerequisites": ["A"]},
        {"id": "C", "kind": "Task", "prerequisites": ["B"]},
    ]

    assert find_node_in_config(config_list, node) == result  # type: ignore[comparison-overlap]
