"""Airflow DAG that uses Google Cloud Batch to run the SuSiE Finemapper step for UKB PPP."""

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from orchestration.models.batch import BatchIndexOperatorSpec, BatchJobOperatorSpec
from orchestration.operators.batch import BatchIndexOperator, BatchJobOperator
from orchestration.types import Environment, EnvironmentSpec
from orchestration.utils import find_environment_vars, find_node_in_config, read_yaml_config
from orchestration.utils.common import shared_dag_args, shared_dag_kwargs

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "ukb_ppp_eur_finemapping.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
env_spec: list[EnvironmentSpec] = config["environment_specs"]
env: Environment = config["env"]
sentinels = find_environment_vars(env_spec, env)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Susie Finemap UKB PPP (EUR)",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    index_config = find_node_in_config(config["nodes"], "generate_finemapping_index")
    job_config = find_node_in_config(config["nodes"], "finemapping_batch_job")

    if index_config:
        batch_index = BatchIndexOperator(
            task_id=index_config["id"],
            batch_index_specs=BatchIndexOperatorSpec(**index_config["google_batch_index_specs"]),
        )

    if job_config:
        finemapping_job = BatchJobOperator.partial(
            task_id=job_config["id"],
            job_name="susie-finemapping",
            batch_job_spec=BatchJobOperatorSpec(**job_config["google_batch"]),
        ).expand(batch_index_row=batch_index.output)

        chain(batch_index, finemapping_job)


if __name__ == "__main__":
    dag.test()
