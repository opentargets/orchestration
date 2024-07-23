"""Gwas catalog DAG."""

from __future__ import annotations

from datetime import datetime
from ot_orchestration.task_groups import (
    gwas_catalog_batch_processing,
    gwas_catalog_manifest_preparation,
)

from airflow.decorators import dag

from ot_orchestration import QRCP

from airflow.utils.helpers import chain


RUN_DATE = datetime.today()

config_path = "/opt/airflow/config/config.yaml"
harmonise_script_path = "/opt/airflow/dags/ot_orchestration/"
config = QRCP.from_file(config_path).serialize()


@dag(start_date=RUN_DATE, dag_id="GWAS_Catalog_dag", schedule="@once", params=config)
def gwas_catalog_dag() -> None:
    """GWAS catalog DAG."""
    chain(
        gwas_catalog_manifest_preparation(),
        gwas_catalog_batch_processing(),
    )


gwas_catalog_dag()
