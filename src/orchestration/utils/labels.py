"""Labels for resources in Google Cloud."""

import re
from collections import UserDict
from typing import Any

from airflow.utils.context import Context

from orchestration.utils.common import GCP_PROJECT_PLATFORM, GCP_SERVICE_ACCOUNT


def default_labels(project: str, is_ppp: bool = False) -> dict[str, str]:
    return {
        "team": "open-targets",
        "subteam": "data" if project == GCP_PROJECT_PLATFORM else "genetics",
        "product": "ppp" if is_ppp else "platform",
        "environment": "development" if "dev" in GCP_SERVICE_ACCOUNT else "production",
        "created_by": "unified-pipeline" if project == GCP_PROJECT_PLATFORM else "gentropy-pipelines",
    }


class Labels(UserDict[str, str]):
    """Collection of labels for Google Cloud resources.

    Behaves like a `dict`, includes a set of default labels, and ensures that all
    labels are correctly formatted.

    Refer to the [controlled vocabularies](https://github.com/opentargets/controlled-vocabularies/blob/main/infrastructure.yaml)
    repository for a list of example values.

    Args:
        more_labels: A dict of extra labels to add on top of the defaults.
        repository for a list of valid values. Defaults to "platform".
        project: The GCP project to use for the labels. This will determine the
            content of the default "environment" label. Defaults to
            GCP_PROJECT_PLATFORM.
    """

    def __init__(
        self,
        extra: dict[str, str] | None = None,
        is_ppp: bool = False,
        project: str = GCP_PROJECT_PLATFORM,
    ) -> None:
        self.extra = extra or {}
        self.project = project
        self.is_ppp = is_ppp
        self.label_dict = default_labels(project=project, is_ppp=self.is_ppp)
        self.label_dict.update({k: self.clean_label(v) for k, v in self.extra.items()})
        super().__init__(self.label_dict)

    def clean_label(self, label: str) -> str:
        """Clean a label for use in google cloud.

        According to the docs: The value can only contain lowercase letters, numeric
        characters, underscores and dashes. The value can be at most 63 characters
        long.
        """
        return re.sub(r"[^a-z0-9-_]", "-", label.lower())[0:63]

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key, self.clean_label(str(value)))

    def add_dag_run_id(self, context: Context) -> None:
        """Add the DAG run ID to the labels.

        Args:
            context: Airflow's task rendering context.
        """
        dag_run = context.get("dag_run")
        if dag_run:
            default_run_label = dag_run.run_id
        run_label = context.get("params", {}).get("run_label", default_run_label)
        self["run"] = run_label
