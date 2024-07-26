"""DAG for the Platform Input Support phase of the platform pipeline.

The platform input support phase will run a series of tasks that fetch the input
data for the platform pipeline. Each step is completely independent, and they can
be run in parallel. Each step runs in a Cloud Run job. The steps are defined in the
`pis.yaml` configuration file, and the DAG is created dynamically from that file.
"""

from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunDeleteJobOperator
from airflow.utils.task_group import TaskGroup

from ot_orchestration.operators.cloud_run import (
    CloudRunCreateJobExtendedOperator,
    CloudRunExecuteJobWithLogsOperator,
)
from ot_orchestration.operators.gcs import UploadConfigOperator
from ot_orchestration.utils.common import (
    GCP_PROJECT_PLATFORM,
    GCP_REGION,
    platform_dag_kwargs,
    shared_dag_args,
)
from ot_orchestration.utils.utils import (
    bucket_name,
    bucket_path,
    create_name,
    read_yaml_config,
)

PIS_IMAGE = "europe-west1-docker.pkg.dev/open-targets-eu-dev/platform-input-support-test/platform-input-support-test:latest"
PIS_SPEC = {"cpu": "1", "memory": "4Gi"}
PIS_SACC = "platform-input-support@open-targets-eu-dev.iam.gserviceaccount.com"
PIS_CONFIG_PATH = Path(__file__).parent / "configs" / "pis.yaml"
PIS_CONFIG = read_yaml_config(PIS_CONFIG_PATH)

with DAG(
    dag_id="platform_input_support",
    default_args=shared_dag_args,
    description="Open Targets Platform — platform-input-support",
    **platform_dag_kwargs,
) as dag:
    u = UploadConfigOperator(
        task_id="upload_pis_config",
        project_id=GCP_PROJECT_PLATFORM,
        src=PIS_CONFIG_PATH,
        gcs_url=PIS_CONFIG["gcs_url"] + "/config.yaml",
    )

    for step_name in PIS_CONFIG["steps"]:
        clean_step_name = create_name(dag.dag_id, step_name)
        job_name = clean_step_name + "-{{ run_id | strhash }}"
        gcs_url = PIS_CONFIG["gcs_url"]
        env_vars = {
            "PIS_STEP": step_name,
            "PIS_CONFIG_FILE": f"/config/{bucket_path(gcs_url)}/config.yaml",
        }

        with TaskGroup(group_id=clean_step_name) as t:
            c = CloudRunCreateJobExtendedOperator(
                task_id=f"{clean_step_name}-cloudrun-create",
                project_id=GCP_PROJECT_PLATFORM,
                region=GCP_REGION,
                job_name=job_name,
                image=PIS_IMAGE,
                env_map=env_vars,
                limits_map=PIS_SPEC,
                service_account=PIS_SACC,
                config_bucket=bucket_name(gcs_url),
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

        t.set_upstream(u)
