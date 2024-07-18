"""Airflow boilerplate code that interfaces with Cloud Run operators which can be shared by several DAGs."""

import hashlib
import re

from google.cloud import run_v2


def clean_name(step_name: str) -> str:
    """Clean a string into a valid cloud run job name."""
    clean_step_name = re.sub(r"[^a-z0-9-]", "-", step_name.lower())
    return f"platform-input-support-{clean_step_name}"


def strhash(s: str) -> str:
    """Create a simple hash from a string."""
    return hashlib.sha256(s.encode()).hexdigest()[:5]


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
