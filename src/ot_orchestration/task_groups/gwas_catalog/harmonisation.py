"""Harmonisation task group."""

from airflow.decorators import task_group, task
from ot_orchestration.utils import (
    get_gwas_catalog_dag_params,
    get_config_from_dag_params,
)
import time
from google.cloud.batch_v1 import Environment
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from airflow.operators.python import get_current_context
from airflow.utils.helpers import chain
from ot_orchestration.utils import create_batch_job, create_task_spec
from ot_orchestration.utils.common import GCP_REGION, GCP_PROJECT


@task_group(group_id="gwas_catalog_harmonisation")
def gwas_catalog_harmonisation() -> None:
    """Harmonise raw summary statistics."""
    existing_manifest_paths = GCSListObjectsOperator(
        task_id="list_existing_manifests",
        bucket="{{ params.DAGS.GWAS_Catalog.staging_bucket}}",
        match_glob="**/manifest.json",
    )

    @task(task_id="create_harmonisation_job")
    def harmonisation_job(manifest_paths: list[str]) -> CloudBatchSubmitJobOperator:
        """Create a harmonisation batch job."""
        cfg = get_config_from_dag_params()
        staging_bucket = get_gwas_catalog_dag_params()["staging_bucket"]
        google_batch_params = cfg.get_googlebatch_params("harmonisation")
        resource_specs = google_batch_params["resource_specs"]
        task_specs = google_batch_params["task_specs"]
        policy_specs = google_batch_params["policy_specs"]
        dag_params = get_gwas_catalog_dag_params()
        image = dag_params["genetics_etl_image"]
        commands = ["echo", "$MANIFEST_PATH"]
        task_env = [
            Environment(variables={"MANIFEST_PATH": f"gs://{staging_bucket}/{mp}"})
            for mp in manifest_paths
        ]
        task_spec = create_task_spec(
            image=image,
            commands=commands,
            resource_specs=resource_specs,
            task_specs=task_specs,
        )
        job = create_batch_job(task_spec, task_env, policy_specs)
        print(job)
        CloudBatchSubmitJobOperator(
            task_id="harmonisation_batch_job",
            project_id=GCP_PROJECT,
            region=GCP_REGION,
            job_name=f"harmonisation-job-{time.strftime('%Y%m%d-%H%M%S')}",
            job=job,
            deferrable=False,
        ).execute(context=get_current_context())

    harmonise = harmonisation_job(existing_manifest_paths.output)
    chain(existing_manifest_paths, harmonise)


__all__ = ["gwas_catalog_harmonisation"]
