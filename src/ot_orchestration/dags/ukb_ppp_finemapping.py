"""Airflow DAG that uses Google Cloud Batch to run the SuSie Finemapper step for UKB PPP."""

from __future__ import annotations

from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG

from ot_orchestration.templates.finemapping import (
    FinemappingBatchOperator,
    generate_manifests_for_finemapping,
)
from ot_orchestration.utils import common

COLLECTED_LOCI = (
    "gs://genetics-portal-dev-analysis/dc16/output/ukb_ppp/clean_loci.parquet"
)
MANIFEST_PREFIX = "gs://gentropy-tmp/ukb/manifest"
OUTPUT_BASE_PATH = "gs://gentropy-tmp/ukb/output"
STUDY_INDEX_PATH = "gs://ukb_ppp_eur_data/study_index"


@task
def generate_manifests():
    return generate_manifests_for_finemapping(
        collected_loci=COLLECTED_LOCI,
        manifest_prefix=MANIFEST_PREFIX,
        output_path=OUTPUT_BASE_PATH,
        max_records_per_chunk=100_000,
    )


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — finemap study loci with SuSie",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
) as dag:
    (
        FinemappingBatchOperator.partial(
            task_id="finemapping_batch_job", study_index_path=STUDY_INDEX_PATH
        ).expand(manifest=generate_manifests())
    )
