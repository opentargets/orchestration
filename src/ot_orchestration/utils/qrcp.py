"""Quick and Robust Configuration Parser."""

from __future__ import annotations

from typing import Any, Callable, Literal
from airflow.operators.python import get_current_context

import json
import yaml
from pydantic import BaseModel
from returns.result import Failure, Result, Success
from ot_orchestration.types import (
    Data_Source,
    ConfigParsingFailure,
    DagConfigNotFound,
    Config_Field_Name,
    Batch_Specs,
    ConfigFieldNotFound,
    Manifest_Preparation_Params,
    Spark_Options,
    Base_Type,
)
from pathlib import Path


class ConfigModel(BaseModel):
    """Top level config structure."""

    DAG: Data_Source
    mode: Literal["FORCE", "RESUME", "CONTINUE"]
    steps: StepModel


class StepModel(BaseModel):
    """Step configuration."""

    manifest_preparation: Manifest_Preparation_Params
    batch_processing: ProviderModel


class ProviderModel(BaseModel):
    """Provider configuration."""

    googlebatch: Batch_Specs
    spark: Spark_Options


Config_Parser = Callable[[Any], Result[ConfigModel, ConfigParsingFailure]]


def default_config_parser(
    conf: dict[Any, Any],
) -> Result[ConfigModel, ConfigParsingFailure]:
    """Default configuration parser.

    If the structure of the configuration object provided in the input parameters is correct,
    then the parsed config is returned otherwise Result object with failuire is returned to be
    consumed by caller.

    Args:
        conf (dict[Any, Any]): configuration object

    Returns:
        Result[ConfigModel, ConfigParsingFailure]: parsing result, either Success(ConfigModel)
        or Failure(ConfigParsingFailure)
    """
    match conf:
        case {"DAG": _, "mode": _, "steps": _}:
            return Success(ConfigModel(**conf))
        case _:
            return Failure(
                f"Could not parse config structure {conf} with default config parser."
            )


class QRCP:
    """Initialize QRCP object.

    Args:
        conf (Any): Configuration object.
        parser (Config_Parser, optional): configuration parser. Defaults to default_config_parser.

    Raises:
        TypeError: _description_
    """

    @classmethod
    def from_file(cls, path: Path | str, **kwargs: Any) -> QRCP:
        """Read from a file."""
        with open(path) as cfg:
            match Path(path).suffix:
                case ".yaml" | ".yml":
                    cfg = yaml.safe_load(cfg)
                case _:
                    cfg = json.load(cfg)
            return cls(cfg, **kwargs)

    def __init__(
        self, conf: Any, parser: Config_Parser = default_config_parser
    ) -> None:
        result = parser(conf)
        match result:
            case Success(config):
                self.config = config
            case Failure(msg):
                raise TypeError(msg)

    def get(
        self, field: Config_Field_Name
    ) -> Result[ConfigFieldNotFound, str | list[str] | dict[Any, Any]]:
        """Get field from the configuration."""
        if field not in self.config.model_fields_set:
            return Failure(f"Field {field} not found in config")
        return Success(self.config.model_dump()[field])

    def get_step_params(
        self: QRCP, step: str
    ) -> Result[dict[str, Base_Type], DagConfigNotFound]:
        """Get gentropy dag parameters from existing configuration.

        Returns:
            Result[Manifest_Preparation_Params | ProviderModel, DagConfigNotFound]: Result of dag params lookup.
        """
        inner_cfg = self.config.steps.model_dump()
        if step not in inner_cfg:
            return Failure(f"Field {step} not present in the configuration.")
        return Success(inner_cfg[step])

    def serialize(self) -> dict[str, Any]:
        """Serialize QRCP object.

        https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/serializers.html
        """
        return self.config.model_dump()

    @staticmethod
    def deserialize(data: dict[str, Any], **kwargs: Config_Parser) -> QRCP:
        """Reconstruct from data object.

        https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/serializers.html
        """
        return QRCP(data, **kwargs)

    def to_file(self, path: Path | str) -> None:
        """Write to file."""
        with open(path, "w") as cfg:
            match Path(path).suffix:
                case "yaml" | "yml":
                    yaml.safe_dump(self.serialize(), cfg)
                case "json":
                    json.dump(self.serialize(), cfg)


def get_full_config() -> QRCP:
    """Get full configuration."""
    return QRCP(get_current_context().get("params"))


def get_step_params(step: str) -> dict[str, Base_Type]:
    """Process initial params passed to the DAG and validate with QRCP config."""
    qrcp = get_full_config()
    result = qrcp.get_step_params(step)
    match result:
        case Success(params):
            return params
        case Failure(msg):
            raise TypeError(msg)
        case _:
            raise NotImplementedError


__all__ = ["QRCP", "ConfigModel", "get_step_params", "get_full_config"]
