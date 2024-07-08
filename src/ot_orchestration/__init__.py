"""Quick and Robust Configuration Parser."""

from __future__ import annotations
from returns.result import Result, Failure, Success
from typing import TYPE_CHECKING
import yaml
from pydantic import BaseModel
from typing import Literal, Callable, Any

Dag_Name = Literal["Gentropy"]
Provider_Name = Literal["dataproc", "googlebatch"]
Config_Field_Name = Literal["datasources", "common_params", "providers", "DAGS"]
Config_Dag_Type = Literal["task"]
Data_Source = Literal["GWAS_Catalog", "eQTL_Catalogque", "finngen", "UK_Biobank_PPP"]
ConfigFieldNotFound = str
Base_Type = str | int | float | bool | list[str]
Dag_Params = dict[str, dict[str, Base_Type | list[Base_Type]]]
ConfigParsingFailure = str
GentropyConfigNotFound = str
if TYPE_CHECKING:
    from pathlib import Path


class DagModel(BaseModel):
    """DAG subconfig structure."""
    name: Dag_Name | str
    description: str
    params: Dag_Params | None


class ProviderModel(BaseModel):
    """Provider subconfig structure."""
    name: Provider_Name | str
    params: dict[str, Base_Type] | None


class ConfigModel(BaseModel):
    """Top level config structure."""
    tags: list[Data_Source | str]
    providers: list[ProviderModel]
    DAGS: list[DagModel]


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
        case {"tags": tags, "providers": providers, "DAGS": dags}:
            return Success(ConfigModel(tags=tags, providers=providers, DAGS=dags))
        case _:
            return Failure(
                "Could not parse config structure {conf} with default config parser."
            )


class QRCP:
    """Initialize Quick and Robust Configuration Parser."""

    @classmethod
    def from_file(cls, path: Path | str, **kwargs: Any) -> QRCP:
        """Read from a file."""
        with open(path) as cfg:
            cfg = yaml.safe_load(cfg)
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
    ) -> Result[Dag_Params, GentropyConfigNotFound]:
        """Get gentropy dag parameters from existing configuration.

        Returns:
            Result[Dag_Params], GentropyConfigNotFound]: Result of Gentropy dag params lookup.
        """
        for d in self.config.DAGS:
            if d.name == dag and d.params:
                return Success(d.params)
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
        with open(path) as cfg:
            yaml.safe_dump(self.serialize(), cfg)
