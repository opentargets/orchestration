"""Generic genetics DAG with batch job support."""

import logging
import time

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import chain
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from ot_orchestration.operators.manifest_operators import (
    ManifestFilterOperator,
    ManifestGenerateOperator,
    ManifestReadOperator,
    ManifestSaveOperator,
    ManifestSubmitBatchJobOperator,
)
from ot_orchestration.types import Manifest_Object
from ot_orchestration.utils import IOManager


@task(task_id="end")
def end():
    """Finish the DAG execution."""
    logging.info("FINISHED")

@task(
    task_id="consolidate_manifests",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
def consolidate_manifests(ti: TaskInstance | None = None) -> list[Manifest_Object]:
    """Consolidate manifests from the execution mode branching."""
    params = get_current_context().get("params")
    if params["mode"] == "CONTINUE":
        branch = "generate_staging_output"
    else:
        branch = "filter_failed_manifests"
    manifests = ti.xcom_pull(branch)
    if len(manifests) == 0:
        raise AirflowSkipException("No manifests detected, skipping.")
    return manifests


@task.branch(task_id="begin")
def begin() -> str:
    """Start the DAG execution by choosing the execution mode."""
    logging.info("START")
    params = get_current_context().get("params")
    logging.info(params)
    if params["mode"] == "CONTINUE":
        return "generate_manifests"
    else:
        return "read_existing_manifests"


@task(task_id="collect_task_outcome", multiple_outputs=True)
def collect_task_outcome(manifests: list[Manifest_Object]):
    """Collect the task(s) outcome and return failed and succeded manifests."""
    # we need to re-read the manifests to report the updated status of the tasks
    manifest_paths = [m["manifestPath"] for m in manifests]
    new_manifests: list[Manifest_Object] = IOManager().load_many(manifest_paths)

    failed_manifests = []
    succeded_manifests = []
    status_flag_prefix = "pass"
    for new_manifest in new_manifests:
        # we assume that the initial state is success
        succeded = True
        for key, val in new_manifest.items():
            if key.startswith(status_flag_prefix) and not val:
                succeded = False

        if succeded:
            succeded_manifests.append(new_manifest)
        else:
            failed_manifests.append(new_manifest)

    logging.info("FAILED MANIFESTS %s/%s", len(failed_manifests), len(manifests))
    return {
        "failed_manifests": failed_manifests,
        "succeded_manifests": succeded_manifests,
    }


def generic_genetics_dag():
    """Generic genetics DAG.

    This function is used to create a dynamic DAG based on provided yaml
    configuration. The dag is responsible for creating manifests based
    on a yaml configuration (TBI) and correct manifest parser for
    specific genetics pipeline.
    """
    exec_mode_branch = begin()

    new_manifests = ManifestGenerateOperator(
        task_id="generate_manifests",
        raw_sumstat_path_pattern="{{ params.steps.manifest_preparation.raw_sumstat_path_pattern }}",
        staging_manifest_path_pattern="{{ params.steps.manifest_preparation.staging_manifest_path_pattern }}",
        harmonised_prefix="{{ params.steps.manifest_preparation.harmonised_prefix }}",
        qc_prefix="{{ params.steps.manifest_preparation.qc_prefix }}",
    ).output

    existing_manifests = ManifestReadOperator(
        task_id="read_existing_manifests",
        staging_manifest_path_pattern="{{ params.steps.manifest_preparation.staging_manifest_path_pattern }}",
    ).output

    failed_existing_manifests = ManifestFilterOperator(
        task_id="filter_failed_manifests",
        manifests=existing_manifests,
    ).output

    saved_manifests = ManifestSaveOperator(
        task_id="generate_staging_output",
        manifest_blobs=new_manifests,  # type: ignore
    ).output

    consolidated_manifests = consolidate_manifests()
    batch_job = ManifestSubmitBatchJobOperator(
        task_id="gwas-catalog_batch_job",
        manifests=consolidated_manifests,  # type: ignore
        job_name=f"gwas-catalog-job-{time.strftime('%Y%m%d-%H%M%S')}",
        step="gwas-catalog-etl",
    ).output

    updated_manifests = collect_task_outcome(manifests=consolidated_manifests)

    # MODE == CONTINUE
    chain(
        exec_mode_branch,
        new_manifests,
        saved_manifests,
        consolidated_manifests,
    )

    # MODE == RESUME
    chain(
        exec_mode_branch,
        existing_manifests,
        failed_existing_manifests,
        consolidated_manifests,
    )

    # ALWAYS AFTER
    chain(
        consolidated_manifests,
        batch_job,
        updated_manifests,
        end(),
    )


__all__ = ["generic_genetics_dag"]
