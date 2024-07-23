"""Test QRCP and orchestration config"""

import pytest
from pathlib import Path
from ot_orchestration import QRCP
from ot_orchestration.utils.qrcp import ConfigModel, default_config_parser
from returns.result import Success, Failure


@pytest.fixture
def orchestration_config() -> Path:
    return Path(__file__).parent.parent / "config" / "config.yaml"


def test_qrcp_parser(orchestration_config: Path) -> None:
    """Test QRCP parser with correct config file.

    Ensure config present in `config` dir can be parsed with QRCP."""
    cfg = QRCP.from_file(path=orchestration_config)
    assert isinstance(cfg.config, ConfigModel)
    assert isinstance(cfg.get("DAG"), Success)
    assert isinstance(cfg.get("resume"), Success)
    assert isinstance(cfg.get("force"), Success)
    assert isinstance(cfg.get("steps"), Success)
    assert isinstance(cfg.get("other_key"), Failure)  # type: ignore


def test_qrcp_parser_failure_config() -> None:
    """Test QRCP behavior with incorrectly stuctured config."""
    config = {"config": "some_config"}
    with pytest.raises(TypeError) as e:
        QRCP(conf=config)
    assert e.value.args[0].startswith("Could not parse config structure")
    assert isinstance(default_config_parser(conf=config), Failure)


def test_serialize(orchestration_config: Path) -> None:
    """Test QRCP serialization and deserialization methods."""
    cfg = QRCP.from_file(path=orchestration_config)
    ser = cfg.serialize()
    assert isinstance(ser, dict)
    de = QRCP.deserialize(ser)
    assert isinstance(de, QRCP)


def test_save_qrcp_to_file(orchestration_config: Path, tmp_path: Path) -> None:
    """Test result from saving file with QRC."""
    output = tmp_path / "orchestration_config.yaml"
    cfg = QRCP.from_file(path=orchestration_config)
    cfg.to_file(output)
    assert output.exists()


def test_get_step_params(orchestration_config: Path) -> None:
    """Test getting correct DAG params from QRCP."""
    cfg = QRCP.from_file(orchestration_config)
    assert isinstance(cfg.get_step_params("aaa"), Failure)
    assert isinstance(cfg.get_step_params("batch_processing"), Success)
