"""Types introduced in the library."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal, TypedDict


class DataprocSpecs(TypedDict):
    python_main_module: str
    cluster_init_script: str
    cluster_metadata: dict[str, str]
    cluster_name: str


class Environment(Enum):
    TEST = "Test"
    PROD = "Prod"


class EnvironmentSpec(TypedDict):
    name: Environment
    vars: dict[str, str]


class ConfigNode(TypedDict):
    id: str
    kind: Literal["Task", "TaskGroup"]
    prerequisites: list[str]
    params: dict[str, Any]
    google_batch: Any
    nodes: list[ConfigNode]
    google_batch_index_specs: Any
