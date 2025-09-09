"""Tools config models."""

from __future__ import annotations

from enum import Enum
from pathlib import Path

from pydantic import BaseModel

from orchestration.dags.config.app_config import AppConfig
from orchestration.dags.config.tools.otter import OtterTaskConfig


class ToolSpecs[T: BaseModel]:
    """Specification of the tool."""

    def __init__(self, *, name: str, config_path: Path, validator: type[T]) -> None:
        self.name = name
        """Name of the tool."""
        self.config_path = config_path
        """Specific Path to the tool configuration file."""
        self.validator = validator
        """Pydantic model to validate the tool configuration."""

    def get_config(self) -> T:
        """Get the validated AppConfig for the tool."""
        ac = AppConfig.from_file(self.config_path, validator=self.validator)
        assert ac.validated, "To use the ToolSpecs, the AppConfig must be validated."
        return ac.validated


class Tools[T: BaseModel](Enum):
    GENTROUTILS = ToolSpecs(
        name="gentroutils", config_path=Path(__file__).parent / "gentroutils.yaml", validator=OtterTaskConfig
    )
    PIS = ToolSpecs(name="pis", config_path=Path(__file__).parent / "pis.yaml", validator=OtterTaskConfig)
    PTS = ToolSpecs(name="pts", config_path=Path(__file__).parent / "pts.yaml", validator=OtterTaskConfig)
    ETL = ToolSpecs(name="etl", config_path=Path(__file__).parent / "etl.conf", validator=BaseModel)
    GENTROPY = ToolSpecs(
        name="gentropy", config_path=Path(__file__).parent / "gentropy.yaml", validator=OtterTaskConfig
    )

    @classmethod
    def get(cls, v: str) -> ToolSpecs[T]:
        """Get the tool specification by name."""
        for c in cls.__members__.values():
            if c.name.lower() == v.lower():
                return c.value
        raise ValueError(f"Tool {v} not found. Available tools: {cls.names()}")

    @classmethod
    def names(cls) -> list[str]:
        """Get the list of tool names."""
        return [c.name for c in cls.__members__.values()]
