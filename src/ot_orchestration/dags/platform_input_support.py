"""DAG for the Platform Input Support phase of the platform pipeline."""

from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
)

from ot_orchestration.operators.gce import ComputeEngineRunContainerizedWorkloadSensor
from ot_orchestration.operators.gcs import UploadConfigOperator
from ot_orchestration.operators.platform import PISDiffComputeOperator
from ot_orchestration.utils import clean_name, read_yaml_config
from ot_orchestration.utils.common import (
    GCP_PROJECT_PLATFORM,
    GCP_REGION,
    platform_dag_kwargs,
    shared_dag_args,
)

PIS_IMAGE = "europe-west1-docker.pkg.dev/open-targets-eu-dev/platform-input-support-test/platform-input-support-test:latest"
PIS_SACC = "platform-input-support@open-targets-eu-dev.iam.gserviceaccount.com"
CONFIG_PATH = Path(__file__).parent / "configs" / "pis.yaml"
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
        "keep_failed": Param(
            True,
            type="boolean",
            description="Whether to keep VMs where failed steps ran, so operator can check on them.",
        ),
    },
) as dag:
    steps = list(CONFIG["steps"].keys())

    for step in steps:
        instance_name = f"uo-pis-{clean_name(step)}-{{{{ run_id | strhash }}}}"

        c = PISDiffComputeOperator(
            task_id=f"diff_{step}",
            project_id=GCP_PROJECT_PLATFORM,
            step_name=step,
            local_config_path=CONFIG_PATH,
            upstream_config_url=GCS_URL + "/config.yaml",
            downstream_task_id=f"upload_config_{step}",
            joiner_task_id=f"join_{step}",
        )

        u = UploadConfigOperator(
            task_id=f"upload_config_{step}",
            project_id=GCP_PROJECT_PLATFORM,
            src=CONFIG_PATH,
            dst=GCS_URL + "/config.yaml",
        )

        r = ComputeEngineRunContainerizedWorkloadSensor(
            task_id=f"run_{step}",
            instance_name=instance_name,
            labels={"subteam": "backend", "tool": "pis", "run": dag.params["run"]},
            container_image=PIS_IMAGE,
            container_env={
                "PIS_STEP": step,
                "PIS_CONFIG_FILE": "/config.yaml",
                "PIS_POOL": "16",
            },
            container_service_account=PIS_SACC,
            container_scopes=["https://www.googleapis.com/auth/drive"],
            container_files={f"{GCS_URL}/config.yaml": "/config.yaml"},
            work_disk_size_gb=150,
            deferrable=True,
        )

        d = ComputeEngineDeleteInstanceOperator(
            task_id=f"delete_vm_{step}",
            project_id=GCP_PROJECT_PLATFORM,
            zone=f"{GCP_REGION}-b",
            resource_id=instance_name,
            trigger_rule="none_failed" if dag.params["keep_failed"] else "all_done",
        )

        j = EmptyOperator(task_id=f"join_{step}")

        c >> u >> r >> d >> j
        c >> j
