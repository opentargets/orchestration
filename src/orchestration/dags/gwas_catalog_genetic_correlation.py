"""Airflow DAG for genetic correlation estimation across representative GWAS studies.

Pipeline stages
---------------
1. **Manifest generation** (Dataproc) — ``rg_representative_manifest``
   Reads the study index and heritability estimates, applies quality filters,
   selects one representative study per (diseaseId, ld_ancestry) cell, and
   writes ``all_pairs.csv`` to GCS.

2. **Munge** (Dataproc) — ``ldsc_munge``
   Pre-joins each unique study's summary statistics with LD scores once,
   producing compact per-study parquets.  This avoids redundant I/O in the
   per-pair batch tasks downstream.

3. **Genetic correlation batch** (Google Batch) — ``rg_genetic_correlation``
   The manifest generator reads ``all_pairs.csv``, stages fixed-size chunk CSVs
   to GCS, and hands one chunk per Batch task.  Each task reads pre-munged
   parquets via pyarrow, runs LDSC rg for every pair in the chunk, and writes
   results to GCS.  Already-completed chunks are skipped automatically.
"""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.utils.trigger_rule import TriggerRule

from orchestration.models.batch import BatchIndexOperatorSpec, BatchJobOperatorSpec
from orchestration.operators.batch import BatchIndexOperator, BatchJobOperator
from orchestration.operators.dataproc import CustomClusterConfig
from orchestration.types import Environment, EnvironmentSpec
from orchestration.utils import (
    chain_dependencies,
    find_environment_vars,
    read_yaml_config,
)
from orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from orchestration.utils.dataproc import create_cluster, delete_cluster, submit_gentropy_step

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "gwas_catalog_genetic_correlation.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
env_spec: list[EnvironmentSpec] = config["environment_specs"]
env: Environment = config["env"]
sentinels = find_environment_vars(env_spec, env)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH, sentinels)

logger = logging.getLogger(__name__)

with DAG(
    dag_id="gentropy_genetic_correlation",
    description="Run pairwise genetic correlations across representative GWAS studies using LDSC",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:

    # ── Stage 1 & 2: Dataproc (manifest generation + munge) ──────────────────

    dataproc_tasks: dict = {}
    for step in config["nodes"]:
        dataproc_tasks[step["id"]] = submit_gentropy_step(
            cluster_name=config["dataproc"]["cluster_name"],
            step_name=step["id"],
            params=step["params"],
        )

    chain_dependencies(nodes=config["nodes"], tasks_or_task_groups=dataproc_tasks)

    # Cluster management: create before entry tasks, delete after terminal tasks
    cc = CustomClusterConfig(
        service_account=None,
        internal_ip_only=False,
        **config["dataproc"]["cluster_config"],
    )
    create_task = create_cluster(config["dataproc"]["cluster_name"], cc)
    delete_task = delete_cluster(config["dataproc"]["cluster_name"])
    delete_task.trigger_rule = TriggerRule.ALL_DONE

    prerequisites_referenced = {
        p
        for node in config["nodes"]
        for p in node.get("prerequisites", [])
    }
    for node in config["nodes"]:
        tid = node["id"]
        if not node.get("prerequisites"):
            dataproc_tasks[tid].set_upstream(create_task)
        if tid not in prerequisites_referenced:
            dataproc_tasks[tid].set_downstream(delete_task)

    # ── Stage 3: Google Batch (per-chunk genetic correlations) ────────────────

    batch_index = BatchIndexOperator(
        task_id="rg_genetic_correlation.batch_index",
        batch_index_specs=BatchIndexOperatorSpec(**config["rg_batch_index_specs"]),
    )

    batch_jobs = BatchJobOperator.partial(
        task_id="rg_genetic_correlation.batch_job",
        job_name="up-genetic-correlation",
        batch_job_spec=BatchJobOperatorSpec(**config["rg_batch"]),
    ).expand(batch_index_row=batch_index.output)

    # Sequence: dataproc cluster deleted → batch index → batch jobs
    chain(delete_task, batch_index)
    chain(batch_index, batch_jobs)

if __name__ == "__main__":
    dag.test()
