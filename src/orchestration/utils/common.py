"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum

from orchestration.utils import strhash

if TYPE_CHECKING:
    from typing import Any, Callable

# Cloud configuration.
GCP_PROJECT_GENETICS = "open-targets-genetics-dev"
GCP_PROJECT_PLATFORM = "open-targets-eu-dev"
GCP_REGION = "europe-west1"
GCP_ZONE = "europe-west1-d"
GCP_DATAPROC_IMAGE = "2.2"
GCP_AUTOSCALING_POLICY = "otg-etl"
GCP_EFM_AUTOSCALING_POLICY = "otg-efm"
GENTROPY_CLI_SCRIPT = "gs://genetics_etl_python_playground/initialisation/cli.py"
GENTROPY_CLUSTER_INIT_SCRIPT = "gs://genetics_etl_python_playground/initialisation/install_dependencies_on_cluster.sh"

# CLI configuration.
CLUSTER_CONFIG_DIR = "/config"
CONFIG_NAME = "ot_config"
PYTHON_CLI = "cli.py"

# Shared DAG construction parameters.
shared_dag_args: dict[str, Any] = {
    "owner": "Open Targets Data Team",
    "retries": 0,
}

shared_dag_kwargs: dict[str, Any] = {
    "tags": ["genetics_etl", "experimental"],
    "start_date": pendulum.now(tz="Europe/London").subtract(days=1),
    "schedule": "@once",
    "catchup": False,
}

unified_pipeline_dag_kwargs: dict[str, Any] = {
    "dag_id": "unified_pipeline",
    "description": "Open Targets unified data generation pipeline",
    "catchup": False,
    "schedule": None,
    "tags": [*shared_dag_kwargs["tags"], "platform", "unified_pipeline"],
    "user_defined_filters": {"strhash": strhash},
}

shared_labels: Callable[[str], dict[str, str]] = lambda project: {
    "team": "open-targets",
    "subteam": "backend",
    "environment": "development" if "dev" in project else "production",
    "created_by": "unified-pipeline",
}

genetics_shared_labels: Callable[[str], dict[str, str]] = lambda project: {
    "team": "open-targets",
    "subteam": "genetics",
    "environment": "development" if "dev" in project else "production",
    "created_by": "gentropy-pipelines",
}
