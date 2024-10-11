"""Airflow DAG for GWAS Catalog sumstat harmonisation."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG

from ot_orchestration.operators.batch.operators import (
    BatchIndexOperator,
    GeneticsBatchJobOperator,
)
from ot_orchestration.utils import (
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs

SOURCE_CONFIG_FILE_PATH = (
    Path(__file__).parent / "config" / "gwas_catalog_sumstat_harmonisation.yaml"
)
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)


@task(task_id="begin")
def begin():
    """Starting the DAG execution."""
    logging.info("STARTING")
    logging.info(config)


@task(task_id="end")
def end():
    """Finish the DAG execution."""
    logging.info("FINISHED")


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog Sumstat Harmonisation",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    node_config = find_node_in_config(config["nodes"], "generate_sumstat_index")
    batch_index = BatchIndexOperator(
        task_id=node_config["id"],
        batch_index_specs=node_config["google_batch_index_specs"],
    )
    node_config = find_node_in_config(config["nodes"], "gwas_catalog_harmonisation")
    harmonisation_batch_job = GeneticsBatchJobOperator.partial(
        task_id=node_config["id"],
        job_name="harmonisation",
        google_batch=node_config["google_batch_specs"],
    ).expand(batch_index_row=batch_index.output)

    chain(begin(), batch_index, harmonisation_batch_job, end())


# @task(task_id="collect_task_outcome", multiple_outputs=True)
# def collect_task_outcome(manifests: str):
#     """Collect the task(s) outcome and return failed and succeeded manifests."""
#     # we need to re-read the manifests to report the updated status of the tasks
#     manifest_paths = [m["manifestPath"] for m in manifests]
#     new_manifests: list[ManifestObject] = IOManager().load_many(manifest_paths)

#     failed_manifests = []
#     succeeded_manifests = []
#     pending_manifests = []
#     status_flag_prefix = "status"
#     for new_manifest in new_manifests:
#         if new_manifest[status_flag_prefix] == "pending":
#             pending_manifests.append(new_manifest)
#             continue
#         if new_manifest[status_flag_prefix] == "failure":
#             failed_manifests.append(new_manifest)
#             continue
#         if new_manifest[status_flag_prefix] == "success":
#             succeeded_manifests.append(new_manifest)

#     logging.info("FAILED MANIFESTS %s/%s", len(failed_manifests), len(manifests))
#     return {
#         "failed_manifests": failed_manifests,
#         "succeeded_manifests": succeeded_manifests,
#         "pending_manifests": pending_manifests,
#     }
