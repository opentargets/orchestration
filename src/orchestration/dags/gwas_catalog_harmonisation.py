"""Airflow DAG for GWAS Catalog sumstat harmonisation."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from orchestration.operators.batch.generic import BatchIndexOperator, BatchJobOperator
from orchestration.types import Environment, EnvironmentSpec
from orchestration.utils import find_environment_vars, find_node_in_config, read_yaml_config
from orchestration.utils.common import shared_dag_args, shared_dag_kwargs

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "gwas_catalog_sumstat_harmonisation.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
env_spec: list[EnvironmentSpec] = config["environment_specs"]
env: Environment = config["env"]
sentinels = find_environment_vars(env_spec, env)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)
logger = logging.getLogger(__name__)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog Sumstat Harmonisation",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    index_config = find_node_in_config(config["nodes"], "generate_sumstat_index")
    harmonisation_config = find_node_in_config(config["nodes"], "gwas_catalog_harmonisation")
    if index_config:
        batch_index = BatchIndexOperator(
            task_id=index_config["id"],
            batch_index_specs=index_config["google_batch_index_specs"],
        )
    if harmonisation_config:
        harmonisation_batch_job = BatchJobOperator.partial(
            task_id=harmonisation_config["id"],
            job_name="harmonisation",
            google_batch=harmonisation_config["google_batch"],
        ).expand(batch_index_row=batch_index.output)

        chain(batch_index, harmonisation_batch_job)


if __name__ == "__main__":
    dag.test()
