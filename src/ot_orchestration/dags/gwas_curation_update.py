"""DAG for updating GWAS Catalog curation table."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ot_orchestration.utils import common
from ot_orchestration.utils.dataproc import (
    create_cluster,
    install_dependencies,
    submit_step,
)
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain

CLUSTER_NAME = "otg-gwascatalog-curation"
RUN_DATE = datetime.today().strftime("%Y-%m-%d")

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog curation update",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    update_gwas_curation = submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="ot_gwas_catalog_study_curation",
        task_id="gwas_catalog_curation_update",
        other_args=[
            f"step.gwas_catalog_study_curation_out=gs://genetics_etl_python_playground/input/v2d/GWAS_Catalog_study_curation_{RUN_DATE}.tsv",
        ],
    )

    # DAG description:
    chain(
        create_cluster(CLUSTER_NAME, num_workers=2),
        install_dependencies(CLUSTER_NAME),
        update_gwas_curation,
    )
