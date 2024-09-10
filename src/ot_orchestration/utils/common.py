"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

import pendulum

from ot_orchestration.utils.utils import clean_label, strhash

GENTROPY_VERSION = "0.0.0"

# Cloud configuration.
GCP_PROJECT = "open-targets-genetics-dev"
GCP_PROJECT_PLATFORM = "open-targets-eu-dev"
GCP_REGION = "europe-west1"
GCP_ZONE = "europe-west1-d"
GCP_DATAPROC_IMAGE = "2.1"
GCP_AUTOSCALING_POLICY = "otg-etl"

# Cluster init configuration.
INITIALISATION_BASE_PATH = (
    f"gs://genetics_etl_python_playground/initialisation/{GENTROPY_VERSION}"
)
CONFIG_TAG = f"{INITIALISATION_BASE_PATH}/config.tar.gz"
PACKAGE_WHEEL = (
    f"{INITIALISATION_BASE_PATH}/gentropy-{GENTROPY_VERSION}-py3-none-any.whl"
)
INITIALISATION_EXECUTABLE_FILE = [
    f"{INITIALISATION_BASE_PATH}/install_dependencies_on_cluster.sh"
]

# CLI configuration.
CLUSTER_CONFIG_DIR = "/config"
CONFIG_NAME = "ot_config"
PYTHON_CLI = "cli.py"

# Shared DAG construction parameters.
shared_dag_args = {
    "owner": "Open Targets Data Team",
    "retries": 0,
}

shared_dag_kwargs = {
    "tags": ["genetics_etl", "experimental"],
    "start_date": pendulum.now(tz="Europe/London").subtract(days=1),
    "schedule": "@once",
    "catchup": False,
}

platform_dag_kwargs = {
    "catchup": False,
    "schedule": None,
    "start_date": pendulum.now(tz="Europe/London").subtract(days=1),
    "tags": [
        "platform",
        "experimental",
    ],
    "user_defined_filters": {"strhash": strhash},
}

platform_shared_labels = lambda project: {
    "team": "open-targets",
    "subteam": "backend",
    "product": "platform",
    "environment": "development" if "dev" in project else "production",
    "created_by": "unified-orchestrator",
}


def prepare_labels(
    custom_labels: dict[str, str] = {},
    project: str = GCP_PROJECT_PLATFORM,
) -> dict[str, str]:
    """Prepare labels for use in google cloud.

    Includes a set of default labels, and ensures that all labels are
    correctly formatted.
    note: To use outside platform, a way to override the "product" label should
    be added.

    Args:
        custom_labels (dict[str, str]): Custom labels to add to the default labels.
        project (str): The name of the project. Defaults to GCP_PROJECT_PLATFORM.
    """
    labels = platform_shared_labels(project)
    labels.update(custom_labels)

    return {k: clean_label(v) for k, v in labels.items()}


def convert_params_to_hydra_positional_arg(
    step: dict[str, dict[str, Any]],
) -> list[str] | None:
    """Convert configuration parameters to form that can be passed to hydra step positional arguments.

    In case the step does not have a "params" key, there are no parameters to convert.

    Args:
        step (dict[str, dict[str, Any]]): Config parameters for the step to convert.

    Returns:
        list[str] | None: List of strings that represents the positional arguments for hydra gentropy step.

    Example:
        >>> convert_params_to_hydra_positional_arg({"params": {"sig": 1, "pval": 0.05}})
        ["step.sig=1", "step.pval=0.05"]
        >>> convert_params_to_hydra_positional_arg({"id": "step1"})
        None
    """
    if "params" not in step.keys() or not step["params"]:
        return None
    return [f"step.{k}={v}" for k, v in step["params"].items()]
