"""Staging DAG implementation."""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

from orchestration.dags.config.staging import StagingPipelineConfig

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

    @task(task_id="read_config")
    def read_config() -> str:
        config = StagingPipelineConfig(path=Path(__file__).parent / "gwas_catalog_update.yaml")
        config.logger.info("Loaded config successfully.")
        config.logger.info(config.templated)
        for step in config.templated.validated.steps:
            config.logger.debug(f"Step: {step.name}, depends_on: {step.depends_on},  config: {step.step_config}")

        return ""

    s = read_config()
    d = EmptyOperator(task_id="diff")
    u = EmptyOperator(task_id="upload_config")
    r = EmptyOperator(task_id="run")
    t = EmptyOperator(task_id="stop_vm")
    e = EmptyOperator(task_id="end")

    chain(s, d)
    chain(d, Label("differences found, run step"), u, r, (t, e))
    chain(d, Label("no differences found, skip step"), e)
