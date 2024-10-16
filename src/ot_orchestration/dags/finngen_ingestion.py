"""Airflow DAG for the Preprocess part of the pipeline."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

from ot_orchestration.utils import chain_dependencies, read_yaml_config
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import (
    create_cluster,
    delete_cluster,
    submit_gentropy_step,
)

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "finngen_ingestion.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Finngen Susie Finemapping Results Ingestion",
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
            trigger_rule=TriggerRule.ALL_DONE,
        )
        tasks[step["id"]] = task
    chain_dependencies(nodes=config["nodes"], tasks_or_task_groups=tasks)

    chain(
        create_cluster(
            cluster_name=config["dataproc"]["cluster_name"],
            autoscaling_policy=config["dataproc"]["autoscaling_policy"],
            master_disk_size=config["dataproc"]["master_disk_size"],
            num_workers=config["dataproc"]["num_workers"],
            cluster_init_script=config["dataproc"]["cluster_init_script"],
            cluster_metadata=config["dataproc"]["cluster_metadata"],
        ),
        [t for t in tasks.values()],
        delete_cluster(config["dataproc"]["cluster_name"]),
    )
