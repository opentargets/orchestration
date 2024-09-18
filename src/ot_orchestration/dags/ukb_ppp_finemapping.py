"""Airflow DAG that uses Google Cloud Batch to run the SuSie Finemapper step for UKB PPP."""

from __future__ import annotations

import time
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)

from ot_orchestration.templates.finemapping import finemapping_batch_jobs
from ot_orchestration.utils import common

STUDY_LOCUS_BASE_PATH = (
    "gs://gentropy-tmp/tskir/ukb_ppp_eur_data_collected_patched_2024_07_09"
)
OUTPUT_BASE_PATH = "gs://gentropy-tmp/tskir/finemapping_2024_07_29"
STUDY_INDEX_PATH = "gs://ukb_ppp_eur_data/study_index"
OUTPUT_PATH = "gs://gentropy-tmp/test_finemapped_out"

# Temporary: defining 10 loci in order to test the DAG.
LOCI = [
    "studyId=UKB_PPP_EUR_A1BG_P04217_OID30771_v1/studyLocusId=-4875420583494530062/part-00132-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_A1BG_P04217_OID30771_v1/studyLocusId=2449331910204577420/part-00058-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_A1BG_P04217_OID30771_v1/studyLocusId=2882125006476788651/part-00077-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_A1BG_P04217_OID30771_v1/studyLocusId=5149163189737967785/part-00069-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_A1BG_P04217_OID30771_v1/studyLocusId=7530607523033270690/part-00045-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_A1BG_P04217_OID30771_v1/studyLocusId=7817416827048695229/part-00095-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_AAMDC_Q9H7C9_OID30236_v1/studyLocusId=-3603332164695210634/part-00061-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_AAMDC_Q9H7C9_OID30236_v1/studyLocusId=-3727530566487910400/part-00029-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_AAMDC_Q9H7C9_OID30236_v1/studyLocusId=-771229199423266821/part-00067-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
    "studyId=UKB_PPP_EUR_AAMDC_Q9H7C9_OID30236_v1/studyLocusId=7906770497215611142/part-00074-a492c618-2414-44cc-81bb-853f3cf40ce7.c000.snappy.parquet",
]
INPUT_PATHS = [f"{STUDY_LOCUS_BASE_PATH}/{L}" for L in LOCI]
OUTPUT_PATHS = [f"{OUTPUT_BASE_PATH}/{L}" for L in LOCI]

@task(task_id="finemapping_task")
def finemapping_tasks() -> list[CloudBatchSubmitJobOperator]:
    """Generate a list of Batch job operators to submit finemapping processing."""
    return [
        CloudBatchSubmitJobOperator(
            task_id="finemapping_batch_job_{i}",
            project_id=common.GCP_PROJECT,
            region=common.GCP_REGION,
            job_name=f"finemapping-job-{i}-{time.strftime('%Y%m%d-%H%M%S')}",
            job=batch_job,
            deferrable=False
        )
        for i, batch_job in enumerate(finemapping_batch_jobs(
            study_locus_paths = INPUT_PATHS,
            output_paths = OUTPUT_PATHS,
            study_index_path = STUDY_INDEX_PATH
        ))
    ]

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — finemap study loci with SuSie",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
) as dag:
    (
        finemapping_tasks()
    )
