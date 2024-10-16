"""DAG for updating GWAS Catalog curation table."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import (
    create_cluster,
    delete_cluster,
    submit_gentropy_step,
)

CLUSTER_NAME = "otg-gwascatalog-curation"
PYTHON_MAIN_MODULE = "gs://genetics_etl_python_playground/initialisation/gentropy/szsz-update-package-for-dataproc-run/cli.py"
RUN_DATE = datetime.now().strftime("%Y-%m-%d")

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog curation update",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    update_gwas_curation = submit_gentropy_step(
        cluster_name=CLUSTER_NAME,
        step_name="gwas_catalog_study_curation",
        python_main_module=PYTHON_MAIN_MODULE,
        params={
            "step.catalog_study_files": [
                "gs://gwas_catalog_data/curated_inputs/gwas_catalog_download_studies.tsv",
                "gs://gwas_catalog_data/curated_inputs/gwas_catalog_unpublished_studies.tsv",
            ],
            "step.catalog_ancestry_files": [
                "gs://gwas_catalog_data/curated_inputs/gwas_catalog_download_ancestries.tsv",
                "gs://gwas_catalog_data/curated_inputs/gwas_catalog_unpublished_ancestries.tsv",
            ],
            "step.gwas_catalog_study_curation_file": "gs://gwas_catalg_data//manifests/gwas_catalog_study_curation.tsv",
            "step.gwas_catalog_study_curation_out": f"gs://genetics_etl_python_playground/input/v2d/GWAS_Catalog_study_curation_{RUN_DATE}.tsv",
        },
    )

    # DAG description:
    chain(
        create_cluster(CLUSTER_NAME, num_workers=2),
        update_gwas_curation,
        delete_cluster(CLUSTER_NAME),
    )
