import pytest
from ot_orchestration.utils import time_to_seconds


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
