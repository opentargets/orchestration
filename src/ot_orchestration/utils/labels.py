"""Labels for resources in Google Cloud."""

from __future__ import annotations

import re
from typing import Any, Generic, TypeVar

from airflow.utils.context import Context
from typing_extensions import Self

from ot_orchestration.models.labels import LabelModel

L = TypeVar("L", bound=LabelModel)


class Labels(Generic[L]):
    """A collection of labels for Google Cloud resources.

    To build this object from labels, one has to use one of the existing label models.

    Refer to the `controlled vocabularies <https://github.com/opentargets/controlled-vocabularies/blob/main/infrastructure.yaml>`__
        repository for a list of example values.

    See the `shared_labels` dict in `common.py` module for the default labels.

    Args:
        extra: A dict of extra labels to add on top of the defaults.
        repository for a list of valid values. Defaults to "platform".
        project: The GCP project to use for the labels. This will determine the
            content of the default "environment" label. Defaults to
            GCP_PROJECT_PLATFORM.
    """

    @classmethod
    def from_model(cls, model: L) -> Labels:
        """Build labels from model."""
        return cls(**model.model_dump())

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> Labels:
        """Construct labels from dict."""
        return cls(**d)

    def __init__(self, **kwargs) -> None:
        self.label_dict = kwargs
        self.label_dict.update({k: self.clean_label(v) for k, v in self.label_dict.items()})

    def clean_label(self, label: str) -> str:
        """Clean a label for use in google cloud.

        According to the docs: The value can only contain lowercase letters, numeric
        characters, underscores and dashes. The value can be at most 63 characters
        long.
        """
        return re.sub(r"[^a-z0-9-_]", "-", label.lower())[0:63]

    def add(self, extra: dict[str, Any]) -> None:
        """Add labels to a collection."""
        self.label_dict.update({k: self.clean_label(v) for k, v in extra.items()})

    def as_dict(self) -> dict[str, str]:
        """Return a dict of clean labels."""
        return self.label_dict

    def add_dag_run_label(self, context: Context) -> Self:
        """Add dag_run label to the labels dictionary.

        This method ensures that the added labels are following expected requirements.
        .. warning::
            <b>This method can be only accessed during airflow runtime where the context object is accessible.</b>

        Args:
            context (Context): Airflow context that allows to access the `dag_run`.

        Raises:
            ValueError: When the function is run outside the airflow dag run.
        """
        dag_run = context.get("dag_run")
        if not dag_run:
            raise ValueError("Could not the `dag_run` ensure the context is running within airflow dag run.")
        runtime_labels = Labels.from_dict({"dag_run": dag_run}).as_dict()
        self.add(runtime_labels)
        return self
