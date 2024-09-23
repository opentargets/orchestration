"""Tests for package util functions."""

import pytest
from ot_orchestration.utils import (
    chain_dependencies,
    convert_params_to_hydra_positional_arg,
    find_node_in_config,
    time_to_seconds,
)


@pytest.mark.parametrize(
    ["input", "output"],
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
    ["input", "output"],
    [
        pytest.param(
            {"step": "some_step", "step.b": 2, "+step.c": 3},
            ["step=some_step", "step.b=2", "+step.c=3"],
            id="step configuration",
        ),
        pytest.param(
            {"step": "some_step", "step.b": {"c": 2, "d": 3}},
            ["step=some_step", "step.b.c=2", "step.b.d=3"],
            id="nested dict",
            marks=pytest.mark.xfail(
                reason="Structured configuration not supported yet."
            ),
        ),
    ],
)
def test_convert_params_to_hydra_positional_arg(input: dict, output: list[str]) -> None:
    """Test conversion of dictionary to hydra positional arguments."""
    assert convert_params_to_hydra_positional_arg(input) == output


def test_find_node_in_config() -> None:
    """Test finding a node in a configuration."""
    config = [
        {"id": "A", "kind": "Task", "prerequisites": []},
        {"id": "B", "kind": "Task", "prerequisites": ["A"]},
        {"id": "C", "kind": "Task", "prerequisites": ["B"]},
    ]

    assert find_node_in_config(config, "A") == {
        "id": "A",
        "kind": "Task",
        "prerequisites": [],
    }
    assert find_node_in_config(config, "B") == {
        "id": "B",
        "kind": "Task",
        "prerequisites": ["A"],
    }
    assert find_node_in_config(config, "C") == {
        "id": "C",
        "kind": "Task",
        "prerequisites": ["B"],
    }
    with pytest.raises(KeyError):
        find_node_in_config(config, "D")


@pytest.mark.xfail(reason="Need to mock the Task class and set_upstream method.")
def test_chain_dependencies() -> None:
    """Test chaining dependencies between tasks."""
    nodes = [
        {"id": "A", "kind": "Task", "prerequisites": []},
        {"id": "B", "kind": "Task", "prerequisites": ["A"]},
        {"id": "C", "kind": "Task", "prerequisites": ["B"]},
    ]
    tasks_or_task_groups = {
        "A": "Task A",
        "B": "Task B",
        "C": "Task C",
    }

    chain_dependencies(nodes, tasks_or_task_groups)
    assert tasks_or_task_groups["A"].upstream_task_ids == set()
    assert tasks_or_task_groups["B"].upstream_task_ids == {"A"}
    assert tasks_or_task_groups["C"].upstream_task_ids == {"B"}
