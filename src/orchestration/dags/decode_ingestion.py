"""Airflow DAG to ingest and harmonise deCODE protein summary statistics."""

from __future__ import annotations

from pathlib import Path

from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

from orchestration.dags.config.staging_config import InfrastructureType, StagingPipelineConfig
from orchestration.operators.dataproc import (
    CommandJobBuilder,
    CreateClusterOperator,
    DeleteClusterOperator,
    SubmitJobOperator,
)
from orchestration.task_chain.dataproc import DataprocChain
from orchestration.utils import resource_name
from orchestration.utils.labels import Labels

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "decode_ingestion.yaml"
config = StagingPipelineConfig(config_path=SOURCE_CONFIG_FILE_PATH)


with DAG(
    dag_id=Path(__file__).stem,
    description="Gentropy Pipeline — Ingest deCODE protein summary statistics",
) as dag:
    chain_vault: dict[str, DataprocChain] = {}
    clusters: dict[str, tuple[CreateClusterOperator, DeleteClusterOperator]] = {}
    for step in config.steps:
        match step.infrastructure.type:
            case InfrastructureType.DATAPROC:
                cluster_config = config.clusters.get(step.infrastructure.pointer)
                c, d = clusters.get(step.infrastructure.pointer, (None, None))
                if not c:
                    c = CreateClusterOperator(
                        task_id=f"{step.infrastructure.pointer}_create_cluster",
                        project_id=config.project_id,
                        region=config.region,
                        labels=Labels.from_staging_config(config, step),
                        cluster_name=resource_name(step.infrastructure.pointer),
                        cluster_config=cluster_config,
                    )
                if not d:
                    d = DeleteClusterOperator(
                        task_id=f"{step.infrastructure.pointer}_delete_cluster",
                        project_id=config.project_id,
                        region=config.region,
                        cluster_name=resource_name(step.infrastructure.pointer),
                        trigger_rule=TriggerRule.ALL_SUCCESS,
                    )
                clusters.setdefault(step.infrastructure.pointer, (c, d))

                s = SubmitJobOperator(
                    task_id=step.id,
                    project_id=config.project_id,
                    region=config.region,
                    cluster_name=resource_name(step.infrastructure.pointer),
                    job_config=CommandJobBuilder(
                        main_python_file_uri=config.main_python_file_uri,
                        command=step.command,
                        properties=None,
                        template_context=None,
                    ),
                )
                ch = DataprocChain(
                    node_id=step.infrastructure.pointer,
                    dependency_node_ids=step.prerequisites or [],
                    create_op=c,
                    submit_op=s,
                    delete_op=d,
                )
                chain_vault.setdefault(step.id, ch)

            case _:
                raise NotImplementedError(
                    f"Infrastructure type {step.infrastructure.type} not supported for {dag.dag_id}"
                )
    DataprocChain.chain_dependencies(chain_vault)


if __name__ == "__main__":
    dag.test()
