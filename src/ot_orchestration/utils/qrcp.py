"""Quick and Robust Configuration Parser."""

from __future__ import annotations

from typing import Any, Callable
from airflow.operators.python import get_current_context

import json
import yaml
from pydantic import BaseModel
from returns.result import Failure, Result, Success
from ot_orchestration.types import (
    Data_Source,
    Base_Type,
    ConfigParsingFailure,
    DagConfigNotFound,
    Config_Field_Name,
    Dataproc_Specs,
    Batch_Specs,
    Step_Name,
    ConfigFieldNotFound,
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
    ) -> Result[dict[str, Base_Type], DagConfigNotFound]:
        """Get gentropy dag parameters from existing configuration.

        Returns:
            Result[Dag_Params], DagConfigNotFound]: Result of Gentropy dag params lookup.
        """
        if dag not in self.config.DAGS:
            return Failure(f"field {dag} not present under the DAG configuration")
        return Success(self.config.DAGS[dag])

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

    def get_dataproc_params(self) -> Dataproc_Specs:
        """Get dataproc specification from the DAG config."""
        return self.config.providers.dataproc

    def get_googlebatch_params(self, step_name: Step_Name) -> Batch_Specs:
        """Get googlebatch specification from the DAG config."""
        googlebatch_cfg = self.config.providers.googlebatch
        step_cfg: Batch_Specs = getattr(googlebatch_cfg, step_name)
        return step_cfg


def get_config_from_dag_params() -> QRCP:
    """Process initial params passed to the DAG and validate with QRCP config."""
    config = get_current_context().get("params")
    return QRCP(conf=config)


def get_gwas_catalog_dag_params() -> dict[str, Base_Type]:
    """Get GWAS_Catalog DAG params."""
    cfg = get_config_from_dag_params()
    result = cfg.get_dag_params(dag="GWAS_Catalog")
    match result:
        case Success(cfg):
            return cfg
        case Failure(msg):
            raise ValueError(msg)


__all__ = [
    "QRCP",
    "ConfigModel",
    "get_config_from_dag_params",
    "get_gwas_catalog_dag_params",
]
