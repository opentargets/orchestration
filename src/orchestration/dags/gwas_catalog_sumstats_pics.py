"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""

from pathlib import Path

from airflow.models.dag import DAG

from orchestration.types import Environment, EnvironmentSpec
from orchestration.utils import chain_dependencies, find_environment_vars, read_yaml_config
from orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from orchestration.utils.dataproc import generate_dataproc_task_chain, submit_gentropy_step

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "gwas_catalog_sumstats_pics.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
env_spec: list[EnvironmentSpec] = config["environment_specs"]
env: Environment = config["env"]
sentinels = find_environment_vars(env_spec, env)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    tasks = {}
    for step in config["nodes"]:
        task = submit_gentropy_step(
            cluster_name=config["dataproc"]["cluster_name"],
            step_name=step["id"],
            params=step["params"],
        )
        tasks[step["id"]] = task
    chain_dependencies(nodes=config["nodes"], tasks_or_task_groups=tasks)
    generate_dataproc_task_chain(tasks=list(tasks.values()), **config["dataproc"])


if __name__ == "__main__":
    dag.test()
