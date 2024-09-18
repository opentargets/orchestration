"""Airflow DAG that uses Google Cloud Batch to run the SuSie Finemapper step for UKB PPP."""

from __future__ import annotations

import time
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)

from ot_orchestration.templates.finemapping import (
    finemapping_batch_job,
    generate_manifests_for_finemapping,
)
from ot_orchestration.utils import common

COLLECTED_LOCI = (
    "gs://genetics-portal-dev-analysis/dc16/output/ukb_ppp/clean_loci.parquet"
)
MANIFEST_PREFIX = "gs://gentropy-tmp/ukb/manifest"
OUTPUT_BASE_PATH = "gs://gentropy-tmp/ukb/output"
STUDY_INDEX_PATH = "gs://ukb_ppp_eur_data/study_index"


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — finemap study loci with SuSie",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
) as dag:
    manifests = generate_manifests_for_finemapping(
        collected_loci=COLLECTED_LOCI,
        manifest_prefix=MANIFEST_PREFIX,
        output_path=OUTPUT_BASE_PATH,
    )
    for i, (manifest_path, num_of_tasks) in enumerate(manifests):
        task = CloudBatchSubmitJobOperator(
            task_id=f"finemapping_batch_job_{i}",
            project_id=common.GCP_PROJECT,
            region=common.GCP_REGION,
            job_name=f"finemapping-job-{i}-{time.strftime('%Y%m%d-%H%M%S')}",
            job=finemapping_batch_job(
                study_index_path=STUDY_INDEX_PATH,
                study_locus_manifest_path=manifest_path,
                task_count=num_of_tasks,
            ),
            deferrable=False,
        )
