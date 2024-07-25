"""DAG for the Platform Input Support phase of the platform pipeline.

The platform input support phase will run a series of tasks that fetch the input
data for the platform pipeline. Each step is completely independent, and they can
be run in parallel. Each step runs in a Cloud Run job. The steps are defined in the
`pis.yaml` configuration file, and the DAG is created dynamically from that file.
"""

from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunDeleteJobOperator,
)
from airflow.utils.task_group import TaskGroup

from ot_orchestration.operators.cloud_run import CloudRunExecuteJobWithLogsOperator
from ot_orchestration.utils.cloud_run import create_cloud_run_job
from ot_orchestration.utils.common import (
    GCP_PROJECT_PLATFORM,
    GCP_REGION,
    platform_dag_kwargs,
    shared_dag_args,
)
from ot_orchestration.utils.utils import create_name, read_yaml_config, strhash

PIS_CONFIG_PATH = Path(__file__).parent / "config" / "pis.yaml"
PIS_IMAGE = "europe-west1-docker.pkg.dev/open-targets-eu-dev/platform-input-support-test/platform-input-support-test:latest"
PIS_MACHINE_SPEC = {"cpu": "1", "memory": "512Mi"}
PIS_SACC = "platform-input-support@open-targets-eu-dev.iam.gserviceaccount.com"


def get_steps() -> list[str]:
    """Read the steps from the PIS configuration file."""
    yaml_config = read_yaml_config(PIS_CONFIG_PATH)
    return yaml_config["steps"].keys()


with DAG(
    dag_id="platform_input_support",
    default_args=shared_dag_args,
    description="Open Targets Platform — platform-input-support",
    **platform_dag_kwargs,
) as dag:
    for step_name in get_steps():
        clean_step_name = create_name(dag.dag_id, step_name)
        env_vars = {"PIS_STEP": step_name}
        job_name = clean_step_name + "-{{ run_id | strhash }}"

        with TaskGroup(group_id=clean_step_name):
            c = CloudRunCreateJobOperator(
                task_id=f"{clean_step_name}-cloudrun-create",
                project_id=GCP_PROJECT_PLATFORM,
                region=GCP_REGION,
                job_name=job_name,
                job=create_cloud_run_job(
                    PIS_IMAGE, env_vars, PIS_MACHINE_SPEC, PIS_SACC
                ),
                dag=dag,
            )

            e = CloudRunExecuteJobWithLogsOperator(
                task_id=f"{clean_step_name}-cloudrun-execute",
                project_id=GCP_PROJECT_PLATFORM,
                region=GCP_REGION,
                job_name=job_name,
                dag=dag,
            )

            d = CloudRunDeleteJobOperator(
                task_id=f"{clean_step_name}-cloudrun-delete",
                project_id=GCP_PROJECT_PLATFORM,
                region=GCP_REGION,
                job_name=job_name,
                trigger_rule="all_done",
                dag=dag,
            )

            c >> e >> d
