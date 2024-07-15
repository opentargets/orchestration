"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

import pendulum

# Code version. It has to be repeated here as well as in `pyproject.toml`, because Airflow isn't able to look at files outside of its `dags/` directory.
GENTROPY_VERSION = "0.0.0"

# Cloud configuration.
GCP_PROJECT = "open-targets-genetics-dev"
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
