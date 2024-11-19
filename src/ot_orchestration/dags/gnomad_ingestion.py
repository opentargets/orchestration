"""Airflow DAG to ingest GnomAD variant and LD data."""

from pathlib import Path

from airflow.models.dag import DAG

from ot_orchestration.utils import chain_dependencies, read_yaml_config
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import (
    generate_dataproc_task_chain,
    submit_gentropy_step,
)

config = read_yaml_config(
    Path(__file__).parent / "config" / "gnomad_ingestion.yaml"
)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Gnomad Ingestion",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    tasks = {}
    for step in config["nodes"]:
        task = submit_gentropy_step(
            cluster_name=config["dataproc"]["cluster_name"],
            step_name=step["id"],
            python_main_module=config["dataproc"]["python_main_module"],
            params=step["params"],
        )
        tasks[step["id"]] = task
    chain_dependencies(nodes=config["nodes"], tasks_or_task_groups=tasks)

    dag = generate_dataproc_task_chain(
        tasks=[t for t in tasks.values()],
        **config["dataproc"]
    )
