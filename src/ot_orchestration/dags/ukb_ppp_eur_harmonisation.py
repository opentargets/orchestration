"""Airflow DAG to ingest and harmonize UKB PPP (EUR) data."""

from pathlib import Path

from airflow.models.dag import DAG

from ot_orchestration.utils import read_yaml_config
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import generate_dag, submit_gentropy_step

config = read_yaml_config(
    Path(__file__).parent / "config" / "ukb_ppp_eur_harmonisation.yaml"
)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Ingest UKB PPP (EUR)",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    tasks = []
    for step in config["nodes"]:
        task = submit_gentropy_step(
            cluster_name=config["dataproc"]["cluster_name"],
            step_name=step["id"],
            python_main_module=config["dataproc"]["python_main_module"],
            params=step["params"],
        )
        tasks.append(task)
    dag = generate_dag(cluster_name=config["dataproc"]["cluster_name"], tasks=tasks)
