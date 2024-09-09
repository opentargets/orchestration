"""DAG for the Platform Input Support phase of the platform pipeline."""

from datetime import datetime
from pathlib import Path

from airflow.decorators.task_group import task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
)
from airflow.utils.edgemodifier import Label

from ot_orchestration.operators.gce import ComputeEngineRunContainerizedWorkloadSensor
from ot_orchestration.operators.gcs import UploadFileOperator
from ot_orchestration.operators.platform import PISDiffComputeOperator
from ot_orchestration.utils import clean_name, read_yaml_config
from ot_orchestration.utils.common import (
    GCP_PROJECT_PLATFORM,
    GCP_REGION,
    platform_dag_kwargs,
    shared_dag_args,
)

PIS_IMAGE = "europe-west1-docker.pkg.dev/open-targets-eu-dev/platform-input-support-test/platform-input-support-test"
PIS_SACC = "platform-input-support@open-targets-eu-dev.iam.gserviceaccount.com"
CONFIG_PATH = Path(__file__).parent / "config" / "pis.yaml"
CONFIG = read_yaml_config(CONFIG_PATH)
GCS_URL = CONFIG["gcs_url"]

with DAG(
    dag_id="platform_input_support",
    default_args=shared_dag_args,
    description="Open Targets Platform — platform-input-support",
    **platform_dag_kwargs,
    params={
        "run": Param(
            f"pis-{datetime.now().strftime('%Y%m%d-%H%M')}",
            description="Unique run identifier, will be added as a label to infrastructure resources.",
        ),
        "pis_image_tag": Param(
            "latest",
            description=(
                "The tag of the platform-input-support image to use. Defaults to 'latest', "
                "but it is recommended to use a specific tag. Keep in mind tags do not have the 'v' prefix."
            ),
        ),
    },
) as dag:
    for step in list(CONFIG["steps"].keys()):

        @task_group(group_id=f"pis_{step}")
        def pis_step(step: str) -> None:
            instance_name = f"uo-pis-{clean_name(step)}-{{{{ run_id | strhash }}}}"
            config_filename = f"config-{clean_name(step)}.yaml"

            c = PISDiffComputeOperator(
                task_id=f"diff_{step}",
                step_name=step,
                local_config_path=CONFIG_PATH,
                upstream_config_url=f"{GCS_URL}/{config_filename}",
            )

            u = UploadFileOperator(
                task_id=f"upload_config_{step}",
                src=CONFIG_PATH,
                dst=GCS_URL + "/" + config_filename,
            )

            r = ComputeEngineRunContainerizedWorkloadSensor(
                task_id=f"run_{step}",
                instance_name=instance_name,
                labels={"subteam": "backend", "tool": "pis", "run": dag.params["run"]},
                container_image=f"{PIS_IMAGE}:{dag.params['pis_image_tag']}",
                container_env={
                    "PIS_STEP": step,
                    "PIS_CONFIG_FILE": "/" + config_filename,
                    "PIS_POOL": "16",
                },
                container_service_account=PIS_SACC,
                container_scopes=["https://www.googleapis.com/auth/drive"],
                container_files={f"{GCS_URL}/{config_filename}": "/" + config_filename},
                work_disk_size_gb=150,
                deferrable=True,
            )

            d = ComputeEngineDeleteInstanceOperator(
                task_id=f"delete_vm_{step}",
                project_id=GCP_PROJECT_PLATFORM,
                zone=f"{GCP_REGION}-b",
                resource_id=instance_name,
            )

            j = EmptyOperator(
                task_id=f"join_{step}", trigger_rule="none_failed_min_one_success"
            )

            c >> Label("invalid previous run") >> u >> r >> d >> j
            c >> Label("valid previous run exists, skip run") >> j

        pis_step(step)
