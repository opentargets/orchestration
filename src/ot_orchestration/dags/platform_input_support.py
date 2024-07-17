"""DAG for the Platform Input Support phase of the platform pipeline.

The platform input support phase will run a series of tasks that fetch the input
data for the platform pipeline. Each step is completely independent, and they can
be run in parallel. Each step runs in a Cloud Run job. The steps are defined in the
`pis.yaml` configuration file, and the DAG is created dynamically from that file.
"""

import re
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunDeleteJobOperator,
    CloudRunExecuteJobOperator,
)
from airflow.utils.task_group import TaskGroup
from google.cloud import run_v2

from ot_orchestration.common_airflow import (
    GCP_REGION,
    platform_dag_kwargs,
    shared_dag_args,
)
from ot_orchestration.utils.utils import read_yaml_config

PIS_CONFIG_PATH = Path(__file__).parent / "configs" / "pis.yaml"
PIS_IMAGE = "europe-west1-docker.pkg.dev/open-targets-eu-dev/platform-input-support-test/platform-input-support-test:latest"
PIS_GCP_PROJECT = "open-targets-eu-dev"
PIS_MACHINE_SPEC = {"cpu": "1", "memory": "512Mi"}


def get_steps() -> list[str]:
    """Read the steps from the PIS configuration file."""
    yaml_config = read_yaml_config(PIS_CONFIG_PATH)
    return yaml_config["steps"].keys()


def clean_job_name(step_name: str) -> str:
    """Create a clean job name from the step name."""
    clean_step_name = re.sub(r"[^a-z0-9-]", "-", step_name.lower())
    return f"platform-input-support-{clean_step_name}"


def create_job_instance(step_name: str) -> run_v2.Job:
    """Create a Cloud Run job instance for a given step."""
    job = run_v2.Job()
    limits = PIS_MACHINE_SPEC
    container = run_v2.Container(
        image=PIS_IMAGE,
        resources=run_v2.ResourceRequirements(limits=limits),
        env=[run_v2.EnvVar(name="PIS_STEP", value=step_name)],
    )
    job.template.template.containers.append(container)
    job.template.template.max_retries = 0
    return job


with DAG(
    "platform_input_support",
    default_args=shared_dag_args,
    description="Open Targets Platform — platform-input-support",
    **platform_dag_kwargs,
) as dag:
    for step_name in get_steps():
        job_name = clean_job_name(step_name)

        with TaskGroup(group_id=job_name):
            create_job = CloudRunCreateJobOperator(
                task_id=f"create_cloudrun_job_{step_name}",
                project_id=PIS_GCP_PROJECT,
                region=GCP_REGION,
                job_name=job_name,
                job=create_job_instance(step_name),
                dag=dag,
            )

            execute_job = CloudRunExecuteJobOperator(
                task_id=f"execute_cloudrun_job_{step_name}",
                project_id=PIS_GCP_PROJECT,
                region=GCP_REGION,
                job_name=job_name,
                dag=dag,
            )

            delete_job = CloudRunDeleteJobOperator(
                task_id=f"delete_cloudrun_job_{step_name}",
                project_id=PIS_GCP_PROJECT,
                region=GCP_REGION,
                job_name=job_name,
                trigger_rule="all_done",
                dag=dag,
            )

            create_job >> execute_job >> delete_job
