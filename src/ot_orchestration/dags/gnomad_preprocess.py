"""Airflow DAG for the Preprocess part of the pipeline."""

from __future__ import annotations

from pathlib import Path

from airflow.models.dag import DAG

from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import submit_step, generate_dag

CLUSTER_NAME = "gnomad-preprocess"

ALL_STEPS = [
    "ot_ld_index",
    "ot_variant_annotation",
]


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Preprocess",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    all_tasks = [
        submit_step(cluster_name=CLUSTER_NAME, step_id=step, task_id=step)
        for step in ALL_STEPS
    ]
    dag = generate_dag(cluster_name=CLUSTER_NAME, tasks=all_tasks)
