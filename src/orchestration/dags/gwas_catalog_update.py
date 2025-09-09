"""Staging DAG implementation."""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

from orchestration.dags.config.staging import StagingPipelineConfig
from orchestration.operators.config import StagingPipelineConfigLogOperator

with DAG(
    dag_id="gwas_catalog_update",
    description="Synchronization of GWAS Catalog release data files.",
    catchup=False,
    schedule=None,
    params={"run_label": Param("manual_run")},
    tags=["gwas_catalog", "staging"],
    is_paused_upon_creation=True,
    default_args={
        "owner": "Open Targets Genetics Team",
        "depends_on_past": False,
        "retries": 0,
        "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    },
) as dag:
    config = StagingPipelineConfig(path=Path(__file__).parent / "config" / "gwas_catalog_update.yaml")
    s = StagingPipelineConfigLogOperator(task_id="log_config", config=config)

    # 1. Log the full configuration
    u = EmptyOperator(task_id="upload_config")

    c = EmptyOperator(task_id="create_vm")
    # 2. Crete the VM or cluster
    r = EmptyOperator(task_id="run")
    t = EmptyOperator(task_id="stop_vm")
    e = EmptyOperator(task_id="end")

    chain(s, u)
    chain(u, Label("differences found, run step"), r, (t, e))
    chain(u, Label("no differences found, skip step"), e)
