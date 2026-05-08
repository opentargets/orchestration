"""Types introduced in the library."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal, TypedDict

import bashlex
from google.cloud.batch_v1 import Runnable
from pydantic import BaseModel, StringConstraints, model_validator


class ManifestObject(TypedDict):
    studyId: str
    rawPath: str
    harmonisedPath: str
    passHarmonisation: bool | None
    passQC: bool | None
    qcPath: str
    manifestPath: str
    studyType: str | None
    analysisFlag: str | None
    isCurated: bool | None
    pubmedId: str | None
    status: Literal["success", "failure", "pending"]


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
    google_batch: GoogleBatchSpecs
    nodes: list[ConfigNode]
    google_batch_index_specs: GoogleBatchIndexSpecs
