"""Test common functions."""

import pytest
from ot_orchestration.utils.common import convert_params_to_hydra_positional_arg


@pytest.mark.parametrize(
    [
        "step_config",
        "output",
    ],
    [
        pytest.param(
            {"id": "step1", "params": {"param1": "value1"}},
            ["step.param1=value1"],
            id="Step with one parameter.",
        ),
        pytest.param(
            {"id": "step1"},
            None,
            id="Step without parameters.",
        ),
        pytest.param(
            {"id": "step1", "params": {}},
            None,
            id="Step with zero parameters.",
        ),
        pytest.param(
            {"id": "step1", "params": {"param1": "value1", "params2": "value2"}},
            ["step.param1=value1", "step.params2=value2"],
            id="Step with multiple parameters.",
        ),
    ],
)
def test_convert_params_to_hydra_positional_arg(
    step_config: dict, output: list[str] | None
) -> None:
    """Test conversion between step configuration and hydra positional arguments."""
    assert convert_params_to_hydra_positional_arg(step_config) == output
