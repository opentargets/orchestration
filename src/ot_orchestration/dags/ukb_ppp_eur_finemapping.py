"""Airflow DAG to finemap UKB PPP (EUR) data."""

from pathlib import Path

from airflow.models.dag import DAG

from ot_orchestration.operators.batch.finemapping import (
    FinemappingBatchJobManifestOperator,
    FinemappingBatchOperator,
)
from ot_orchestration.utils import (
    chain_dependencies,
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs

config = read_yaml_config(
    Path(__file__).parent / "config" / "ukb_ppp_eur_finemapping.yaml"
)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — Susie Finemap UKB PPP (EUR)",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    tasks = {}

    task_config = find_node_in_config(config["nodes"], "generate_manifests")
    generate_manifests = FinemappingBatchJobManifestOperator(
        task_id=task_config["id"],
        **task_config["params"],
    )

    task_config = find_node_in_config(config["nodes"], "finemapping_batch_job")
    finemapping_job = FinemappingBatchOperator.partial(
        task_id=task_config["id"],
        study_index_path=task_config["params"]["study_index_path"],
        google_batch=task_config["google_batch"],
    ).expand(manifest=generate_manifests.output)

    tasks[generate_manifests.task_id] = generate_manifests
    tasks[finemapping_job.task_id] = finemapping_job

    chain_dependencies(nodes=config["nodes"], tasks_or_task_groups=tasks)
