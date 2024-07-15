"""Quick and Robust Configuration Parser."""

from __future__ import annotations

from typing import Any, Callable

import json
import yaml
from pydantic import BaseModel
from returns.result import Failure, Result, Success
from ot_orchestration.types import (
    Data_Source,
    Base_Type,
    ConfigParsingFailure,
    ConfigFieldNotFound,
    Config_Field_Name,
    Dataproc_Specs,
    Batch_Specs,
)
from pathlib import Path


class ConfigModel(BaseModel):
    """Top level config structure."""

    providers: ProviderModel
    DAGS: dict[Data_Source, dict[str, Base_Type]]


class ProviderModel(BaseModel):
    """Provider configuration."""

    googlebatch: StepModel
    dataproc: Dataproc_Specs


class StepModel(BaseModel):
    """Step configuration."""

    harmonisation: Batch_Specs


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
        case {"providers": providers, "DAGS": dags}:
            return Success(ConfigModel(providers=providers, DAGS=dags))
        case _:
            return Failure(
                f"Could not parse config structure {conf} with default config parser."
            )


class QRCP:
    """Initialize Quick and Robust Configuration Parser."""

    @classmethod
    def from_file(cls, path: Path | str, **kwargs: Any) -> QRCP:
        """Read from a file."""
        with open(path) as cfg:
            match Path(path).suffix:
                case "yaml" | "yml":
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

    def get_dag_params(
        self: QRCP, dag: Data_Source | str
    ) -> Result[dict[str, Base_Type], ConfigFieldNotFound]:
        """Get gentropy dag parameters from existing configuration.

        Returns:
            Result[Dag_Params], ConfigFieldNotFound]: Result of Gentropy dag params lookup.
        """
        for d in self.config.DAGS:
            if d == dag:
                return Success(self.config.DAGS[d])
        return Failure(f"{dag} Config was not found")

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


__all__ = ["QRCP", "ConfigModel"]
