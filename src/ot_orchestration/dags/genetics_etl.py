"""Genetics ETL dag."""

from __future__ import annotations

import time
from pathlib import Path

from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from ot_orchestration.operators.batch.vep import VepAnnotateOperator
from ot_orchestration.types import Environment, EnvironmentSpec
from ot_orchestration.utils import (
    chain_dependencies,
    find_environment_vars,
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import (
    generate_dataproc_task_chain,
    submit_gentropy_step,
)
from ot_orchestration.utils.labels import GentropyDagLabels

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "genetics_etl.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
env_spec: list[EnvironmentSpec] = config["environment_specs"]
env: Environment = config["env"]
sentinels = find_environment_vars(env_spec, env)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)
nodes = config["nodes"]
node_map: dict[str, BaseOperator] = {}


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as genetics_etl_dag:
    with TaskGroup(group_id="genetics_etl") as genetics_etl:
        task_config = find_node_in_config(nodes, "variant_annotation")
        if task_config:
            task_config_params = task_config["params"]
            variant_annotation = VepAnnotateOperator(
                task_id=task_config["id"],
                vcf_input_path=task_config_params["vcf_input_path"],
                vep_output_path=task_config_params["vep_output_path"],
                vep_cache_path=task_config_params["vep_cache_path"],
                google_batch=task_config["google_batch"],
                job_name=f"vep-job-{time.strftime('%Y%m%d-%H%M%S')}",
            )

            node_map["variant_annotation"] = variant_annotation

        tasks = [node for node in nodes if "google_batch" not in node]
        # Build individual tasks and register them as nodes.
        for task in tasks:
            dataproc_specs = config["dataproc"]
            this_task = submit_gentropy_step(
                cluster_name=dataproc_specs["cluster_name"],
                step_name=task["id"],
                params=task["params"],
            )
            node_map[task["id"]] = this_task

        # chain prerequisites
        chain_dependencies(nodes=config["nodes"], tasks_or_task_groups=node_map)
        generate_dataproc_task_chain(
            tasks=list(node_map.values()),
            **config["dataproc"],
            labels=GentropyDagLabels(gentropy_dag="genetics_etl", run_id="airflow"),
        )
