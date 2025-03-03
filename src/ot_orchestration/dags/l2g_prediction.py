"""Airflow DAG for GWAS Catalog sumstat harmonisation."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from ot_orchestration.operators.batch.generic import (
    BatchIndexOperator,
    BatchJobOperator,
)
from ot_orchestration.types import Environment, EnvironmentSpec
from ot_orchestration.utils import (
    find_environment_vars,
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "l2g_prediction.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
env_spec: list[EnvironmentSpec] = config["environment_specs"]
env: Environment = config["env"]
sentinels = find_environment_vars(env_spec, env)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — L2G Prediction and Explanation",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    index_config = find_node_in_config(config["nodes"], "build_l2g_prediction")
    prediction_config = find_node_in_config(config["nodes"], "run_l2g_prediction")

    if index_config and prediction_config:
        batch_index = BatchIndexOperator(
            task_id=index_config["id"],
            batch_index_specs=index_config["google_batch_index_specs"],
        )
        prediction_task = BatchJobOperator.partial(
            task_id=prediction_config["id"],
            job_name="prediction",
            google_batch=prediction_config["google_batch"],
        ).expand(batch_index_row=batch_index.output)

        chain(batch_index, prediction_task)
