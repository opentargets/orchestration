"""Airflow DAG to ingest and harmonise FinnGen UKB meta-analysis data."""

from pathlib import Path

from airflow.models.dag import DAG

from ot_orchestration.utils import read_yaml_config
from ot_orchestration.utils.common import (
    convert_params_to_hydra_positional_arg,
    shared_dag_args,
    shared_dag_kwargs,
)
from ot_orchestration.utils.dataproc import generate_dag, submit_step

config = read_yaml_config(
    Path(__file__).parent / "config" / "finngen_ukb_meta_harmonisation.yaml"
)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Ingest FinnGen UKB meta-analysis",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    tasks = []
    for step in config["steps"]:
        step_id = step["id"]
        step_params = convert_params_to_hydra_positional_arg(step)
        task = submit_step(
            cluster_name=config["cluster_name"],
            step_id=step_id,
            task_id=step_id,
            other_args=step_params,
        )
        tasks.append(task)
    dag = generate_dag(cluster_name=config["cluster_name"], tasks=tasks)
