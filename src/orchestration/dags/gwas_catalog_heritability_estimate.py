"""Airflow DAG for heritability estimation of harmonised summary statistics."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from orchestration.models.batch import BatchIndexOperatorSpec, BatchJobOperatorSpec
from orchestration.operators.batch import BatchIndexOperator, BatchJobOperator
from orchestration.types import Environment, EnvironmentSpec
from orchestration.utils import find_environment_vars, find_node_in_config, read_yaml_config
from orchestration.utils.common import shared_dag_args, shared_dag_kwargs

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "gwas_catalog_heritability_estimate.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
env_spec: list[EnvironmentSpec] = config["environment_specs"]
env: Environment = config["env"]
sentinels = find_environment_vars(env_spec, env)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)
logger = logging.getLogger(__name__)

with DAG(
    dag_id="gentropy_heritability_estimate",
    description="Run heritability estimation for harmonised summary statistics using gentropy",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    step_config = find_node_in_config(config["nodes"], "heritability_estimate")

    if step_config:
        batch_index = BatchIndexOperator(
            task_id="heritability_estimate.batch_index",
            batch_index_specs=BatchIndexOperatorSpec(**step_config["google_batch_index_specs"]),
        )

        batch_jobs = BatchJobOperator.partial(
            task_id="heritability_estimate.batch_job",
            job_name="up-heritability-estimate",
            batch_job_spec=BatchJobOperatorSpec(**step_config["google_batch"]),
        ).expand(batch_index_row=batch_index.output)

        chain(batch_index, batch_jobs)

if __name__ == "__main__":
    dag.test()
