"""Airflow DAG for the Preprocess part of the pipeline."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

from ot_orchestration.utils import common
from ot_orchestration.utils.dataproc import (
    create_cluster,
    delete_cluster,
    install_dependencies,
    submit_step,
)

EFO_MAPPINGS_PATH = "https://raw.githubusercontent.com/opentargets/curation/24.09.1/mappings/disease/manual_string.tsv"
STUDY_INDEX_OUT = "gs://finngen_data/r11/study_index"
CREDIBLE_SETS_SUMMARY_IN = (
    "gs://finngen-public-data-r11/finemap/summary/*.cred.summary.tsv"
)
SNP_IN = "gs://finngen-public-data-r11/finemap/full/susie/*.snp.bgz"
FINNGEN_PREFIX = "FINNGEN_R11_"
FINEMAPPING_OUT = "gs://finngen_data/r11/finemapping"

CLUSTER_NAME = "otg-finemapping-ingestion-finngen"
AUTOSCALING = "finngen-preprocess"

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen Susie Finemapping Results Ingestion",
    default_args=common.shared_dag_args,
    **common.shared_dag_kwargs,
):
    finngen_finemapping_ingestion = submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="finngen_finemapping_ingestion",
        task_id="finngen_finemapping_ingestion",
        other_args=[
            f"step.finngen_finemapping_out={FINEMAPPING_OUT}",
            f"step.finngen_release_prefix={FINNGEN_PREFIX}",
            f"step.finngen_susie_finemapping_snp_files={SNP_IN}",
            f"step.finngen_susie_finemapping_cs_summary_files={CREDIBLE_SETS_SUMMARY_IN}",
            "step.session.start_hail=true",
            "step.session.write_mode=overwrite",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    finngen_study_index = submit_step(
        cluster_name=CLUSTER_NAME,
        step_id="finngen_studies",
        task_id="finngen_studies",
        other_args=[
            f"step.finngen_study_index_out={STUDY_INDEX_OUT}",
            "step.session.write_mode=overwrite",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )
    chain(
        create_cluster(
            CLUSTER_NAME,
            autoscaling_policy=AUTOSCALING,
            master_disk_size=2000,
            num_workers=6,
        ),
        install_dependencies(CLUSTER_NAME),
        finngen_study_index,
        finngen_finemapping_ingestion,
        delete_cluster(CLUSTER_NAME),
    )
