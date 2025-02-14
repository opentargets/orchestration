"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum

from ot_orchestration.utils import strhash

if TYPE_CHECKING:
    from typing import Any

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
DATAPROC_BASE_PROPERTIES = {
    "spark:spark.sql.adaptive.enabled": "true",
    "spark:spark.shuffle.service.enabled": "true",
}

DATAPROC_EFM_MODE_PROPERTIES = {
    "dataproc:efm.spark.shuffle": "primary-worker",
    "spark:spark.sql.files.maxPartitionBytes": "1073741824",  # value proposed by the Dataproc documentation. See EFM in docstring.
    "yarn:spark.shuffle.io.serverThreads": "50",  # ensure more threads can write default for n-standard-16 is 2 * (16 cores) threads
    "spark:spark.shuffle.io.numConnectionsPerPeer": "5",
    "spark:spark.stage.maxConsecutiveAttempts": "10",  # defaults to 4, this is in case the master was lost
    "spark:spark.task.maxFailures": "10",
}

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
