"""Test common functions."""

from typing import Any

import pytest
from ot_orchestration.utils import convert_params_to_hydra_positional_arg


@pytest.mark.parametrize(
    [
        "params",
        "output",
        "error",
    ],
    [
        pytest.param(
            {"step.param1": "value1", "step.param2": "value2"},
            ["step.param1=value1", "step.param2=value2"],
            False,
            id="Step with two parameters.",
        ),
        pytest.param(
            [],
            None,
            ValueError,
            id="Step without parameters.",
        ),
        pytest.param(
            {"+step.param1": "value1"},
            ["+step.param1=value1"],
            False,
            id="Step with + parameter.",
        ),
    ],
)
def test_convert_params_to_hydra_positional_arg(
    params: dict[str, Any] | None, output: list[str], error: Any
) -> None:
    """Test conversion between step configuration and hydra positional arguments."""
    if error:
        with pytest.raises(error):
            convert_params_to_hydra_positional_arg(params)
    else:
        assert convert_params_to_hydra_positional_arg(params) == output
