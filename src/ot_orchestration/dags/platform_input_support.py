"""DAG for the Platform Input Support phase of the platform pipeline.

The platform input support phase will run a series of tasks that fetch the input
data for the platform pipeline. Each step is completely independent, and they can
be run in parallel. Each step runs in a Cloud Run job. The steps are defined in the
`pis.yaml` configuration file, and the DAG is created dynamically from that file.
"""

import hashlib
import re
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunDeleteJobOperator,
)
from airflow.utils.task_group import TaskGroup
from google.cloud import run_v2

from ot_orchestration.common_airflow import (
    GCP_REGION,
    platform_dag_kwargs,
    shared_dag_args,
)
from ot_orchestration.operators.cloud_run_fetch_logs_operator import (
    CloudRunExecuteJobWithLogsOperator,
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


def clean_name(step_name: str) -> str:
    """Clean the PIS step name."""
    clean_step_name = re.sub(r"[^a-z0-9-]", "-", step_name.lower())
    return f"platform-input-support-{clean_step_name}"


def hash(run_id: str) -> str:
    """Create a hash from the run ID."""
    return hashlib.sha256(run_id.encode()).hexdigest()[:5]


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
        name = clean_name(step_name)

        with TaskGroup(group_id=name):
            task_id = f"create_cloudrun_job_{step_name}"

            @task(task_id=task_id)
            def create_job(task_instance: TaskInstance | None = None):
                """Create a Cloud Run job."""
                job_name = f"{name}-{hash(task_instance.run_id)}"
                c = CloudRunCreateJobOperator(
                    task_id=task_id,
                    project_id=PIS_GCP_PROJECT,
                    region=GCP_REGION,
                    job_name=job_name,
                    job=create_job_instance(step_name),
                    dag=dag,
                )
                c.execute(context=task_instance.get_template_context())

            task_id = f"execute_cloudrun_job_{step_name}"

            @task(task_id=task_id)
            def execute_job(task_instance: TaskInstance | None = None):
                """Execute a Cloud Run job."""
                job_name = f"{name}-{hash(task_instance.run_id)}"
                e = CloudRunExecuteJobWithLogsOperator(
                    task_id=task_id,
                    project_id=PIS_GCP_PROJECT,
                    region=GCP_REGION,
                    job_name=job_name,
                    dag=dag,
                )
                e.execute(context=task_instance.get_template_context())

            task_id = f"delete_cloudrun_job_{step_name}"

            @task(task_id=task_id)
            def delete_job(task_instance: TaskInstance | None = None):
                """Delete a Cloud Run job."""
                job_name = f"{name}-{hash(task_instance.run_id)}"
                d = CloudRunDeleteJobOperator(
                    task_id=task_id,
                    project_id=PIS_GCP_PROJECT,
                    region=GCP_REGION,
                    job_name=job_name,
                    trigger_rule="all_done",
                    dag=dag,
                )
                d.execute(context=task_instance.get_template_context())

            create_job() >> execute_job() >> delete_job()
