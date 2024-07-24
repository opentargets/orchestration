"""Batch job processing task group."""

from airflow.decorators import task_group, task
from ot_orchestration.utils import get_step_params
from google.cloud.batch_v1 import Environment
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from airflow.utils.helpers import chain
from ot_orchestration.utils import create_batch_job, create_task_spec
from ot_orchestration.utils import GCSPath
from airflow.models import TaskInstance
import logging


@task_group(group_id="gwas_catalog_batch_processing")
def gwas_catalog_batch_processing() -> None:
    """This task group includes harmonisation, qc and clumping as a single batch job."""

    @task(task_id="get_manifests_from_preparation", multiple_outputs=True)
    def get_batch_task_inputs(
        task_instance: TaskInstance | None = None,
    ) -> list[GCSPath]:
        """Get manifests from preparation step."""
        manifest_paths = task_instance.xcom_pull(
            task_ids="manifest_preparation.choose_manifest_paths"
        )
        config_path = task_instance.xcom_pull(
            task_ids="manifest_preparation.save_config"
        )
        logging.info("MANIFEST PATHS: %s", manifest_paths)
        logging.info("CONFIG PATH: %s", config_path)
        return {"manifest_paths": manifest_paths, "config_path": config_path}

    batch_inputs = get_batch_task_inputs()

    @task(task_id="batch_job", multiple_outputs=True)
    def execute_batch_job(
        manifest_paths: list[str], config_path: str
    ) -> CloudBatchSubmitJobOperator:
        """Create a harmonisation batch job."""
        params = get_step_params("batch_processing")
        logging.info("PARAMS: %s", params)
        google_batch_params = params["googlebatch"]
        resource_specs = google_batch_params["resource_specs"]
        task_specs = google_batch_params["task_specs"]
        policy_specs = google_batch_params["policy_specs"]
        image = google_batch_params["image"]
        commands = google_batch_params["commands"]
        logging.info("MANIFEST_PATHS: %s", manifest_paths)
        task_env = [
            Environment(variables={"MANIFEST_PATH": mp}) for mp in manifest_paths
        ]
        task_spec = create_task_spec(
            image=image,
            commands=commands,
            resource_specs=resource_specs,
            task_specs=task_specs,
        )
        job = create_batch_job(task_spec, task_env, policy_specs)
        logging.info("JOB: %s", job)
        # CloudBatchSubmitJobOperator(
        #     task_id="harmonisation_batch_job",
        #     project_id=GCP_PROJECT,
        #     region=GCP_REGION,
        #     job_name=f"harmonisation-job-{time.strftime('%Y%m%d-%H%M%S')}",
        #     job=job,
        #     deferrable=False,
        # ).execute(context=get_current_context())

    batch_job = execute_batch_job(
        batch_inputs["manifest_paths"], batch_inputs["config_path"]
    )
    chain(batch_inputs, batch_job)


__all__ = ["gwas_catalog_batch_processing"]
