"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

# Cloud configuration.
GCP_PROJECT_PLATFORM = "open-targets-eu-dev"
GCP_PROJECT_GENETICS = "open-targets-genetics-dev"
GCP_SERVICE_ACCOUNT = "up-airflow-dev@open-targets-eu-dev.iam.gserviceaccount.com"
GCP_REGION = "europe-west1"
GCP_ZONE = "europe-west1-d"


shared_dag_args: dict[str, Any] = {
    "owner": "Open Targets Data Team",
    "retries": 0,
}

shared_dag_kwargs: dict[str, Any] = {
    "tags": ["genetics_etl", "experimental"],
    "start_date": datetime.now(tz=UTC) - timedelta(days=1),
    "schedule": "@once",
    "catchup": False,
}
