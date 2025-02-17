"""Gentropy pipeline configuration model."""

from typing import Literal

from pydantic import BaseModel, model_validator
from typing_extensions import Self

from ot_orchestration.models.dataproc import (
    DataprocClusterCreateModel,
    DataprocClusterDeleteModel,
    DataprocSubmitPysparkJobModel,
    DataprocSubmitSparkJobModel,
)
from ot_orchestration.models.google_batch import GoogleBatchSpecsModel


class EnvironmentDefinition(BaseModel):
    """Model defining the environment definition required fields."""

    name: Literal["test", "staging"]
    variables: dict[str, str]


class EnvironmentSpecs(BaseModel):
    """Model defining template for allowed runtime environments."""

    environments: list[EnvironmentDefinition]


class GentropyPipelineTaskModel(BaseModel):
    id: str
    prerequisites: list[str]
    task: (
        DataprocClusterCreateModel
        | DataprocSubmitSparkJobModel
        | DataprocSubmitPysparkJobModel
        | DataprocClusterDeleteModel
        | GoogleBatchSpecsModel
    )


class GentropyPipelineTaskGroupModel(BaseModel):
    prerequisites: list[str]
    task_group: list[GentropyPipelineTaskModel]


class GentropyPipelineConfigModel(BaseModel):
    """Generic model for gentropy Pipeline."""

    environment_specs: EnvironmentSpecs
    environment: Literal["test", "staging"]
    tasks: list[GentropyPipelineTaskGroupModel | GentropyPipelineTaskModel]

    @model_validator(mode="after")
    def validate_prerequisites(self) -> Self:
        """Validate the prerequisite tasks.

        1. Check recursively within the task groups
        """
        depencency_tree = {}
        # 1. find the root of the tree
        roots = []
        for n in self.tasks:
            match n:
                case GentropyPipelineTaskGroupModel(prerequisites=[]):
                    roots.append(n)
                case GentropyPipelineTaskModel(prerequisites=[]):
                    roots.append(n)

        return self
