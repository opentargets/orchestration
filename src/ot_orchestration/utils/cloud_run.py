"""Airflow utilities for Cloud Run operators which can be shared by several DAGs."""

from google.cloud import run_v2


def create_cloud_run_job(
    image: str, env: dict[str, str], limits: dict[str, str]
) -> run_v2.Job:
    """Create a Cloud Run job instance for a given step."""
    job = run_v2.Job()
    env = [run_v2.EnvVar({"name": k, "value": v}) for k, v in env.items()]
    container = run_v2.Container(
        image=image,
        resources=run_v2.ResourceRequirements(limits=limits),
        env=env,
    )
    job.template.template.containers.append(container)
    job.template.template.max_retries = 0
    return job
