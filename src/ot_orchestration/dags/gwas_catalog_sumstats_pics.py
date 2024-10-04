"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from ot_orchestration.utils import (
    chain_dependencies,
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import (
    create_cluster,
    delete_cluster,
    submit_gentropy_step,
    reinstall_dependencies,
)

CONFIG_PATH = Path(__file__).parent / "config" / "gwas_catalog_sumstats_pics.yaml"
config = read_yaml_config(CONFIG_PATH)
sumstat_config = find_node_in_config(config["nodes"], "summary_statistics_processing")

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:

    # Processing summary statistics from GWAS Catalog:
    with TaskGroup(group_id=sumstat_config["id"]) as summary_statistics_processing:
        tasks = {}
        if sumstat_config["nodes"]:
            for step in sumstat_config["nodes"]:
                task = submit_gentropy_step(
                    cluster_name=config["dataproc"]["cluster_name"],
                    step_name=step["id"],
                    python_main_module=config["dataproc"]["python_main_module"],
                    params=step["params"],
                )
                tasks[step["id"]] = task
            chain_dependencies(
                nodes=sumstat_config["nodes"], tasks_or_task_groups=tasks
            )  # type: ignore

    # DAG description:
    chain(
        create_cluster(
            cluster_name=config["dataproc"]["cluster_name"],
            autoscaling_policy=config["dataproc"]["autoscaling_policy"],
            num_workers=config["dataproc"]["num_workers"],
            cluster_metadata=config["dataproc"]["cluster_metadata"],
            cluster_init_script=config["dataproc"]["cluster_init_script"],
        ),
        reinstall_dependencies(
            cluster_name=config["dataproc"]["cluster_name"],
            cluster_init_script=config["dataproc"]["cluster_init_script"],
        ),
        summary_statistics_processing,
        delete_cluster(config["dataproc"]["cluster_name"]),
    )

if __name__ == "__main__":
    pass
