"""Airflow DAG for the preprocessing of GWAS Catalog's top hits."""

from __future__ import annotations

from pathlib import Path

from airflow.models.dag import DAG

from orchestration.dags.config.staging_config import StagingConfig
from orchestration.utils import chain_dependencies
from orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from orchestration.utils.dataproc import generate_dataproc_task_chain, submit_gentropy_step

staging_config = StagingConfig(dag_name=Path(__file__).stem)


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets GWAS Catalog top hits (Curated Associations) processing",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    # Processing curated GWAS Catalog top-bottom:
    tasks = {}
    cluster_config = staging_config.clusters.get("staging_tophits")
    for step in staging_config.st.get("steps"):
        task = submit_gentropy_step(
            cluster_name="tophits",
            step_name=step["id"],
            params=step["params"],
        )
        tasks[step["id"]] = task
    chain_dependencies(nodes=staging_config.st.get("steps"), tasks_or_task_groups=tasks)  # type: ignore
    generate_dataproc_task_chain(tasks=list(tasks.values()), **cluster_config)

if __name__ == "__main__":
    pass
