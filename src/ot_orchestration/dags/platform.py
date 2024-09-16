"""DAG for the Open Targets Platform pipeline."""

from datetime import datetime

from airflow.decorators.task_group import task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
)
from airflow.utils.edgemodifier import Label

from ot_orchestration.dags.config.platform import PlatformConfig
from ot_orchestration.operators.gce import ComputeEngineRunContainerizedWorkloadSensor
from ot_orchestration.operators.gcs import (
    UploadRemoteFileOperator,
    UploadStringOperator,
)
from ot_orchestration.operators.platform import PISDiffComputeOperator
from ot_orchestration.utils import clean_name, to_hocon, to_yaml
from ot_orchestration.utils.common import (
    GCP_PROJECT_PLATFORM,
    GCP_REGION,
    GCP_ZONE,
    platform_dag_kwargs,
    shared_dag_args,
)
from ot_orchestration.utils.dataproc_platform import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)
from ot_orchestration.utils.labels import Labels

with DAG(
    default_args=shared_dag_args,
    **platform_dag_kwargs,
    params={
        "run_label": Param(
            default=f"pis-{datetime.now().strftime('%Y%m%d-%H%M')}",
            description="""A label with key 'run' and the contents of this parameter
                           will be added to any infrastructure resources that this
                           pipeline creates in Google Cloud.""",
        ),
    },
) as dag:
    config = PlatformConfig()
    steps = {}  # this is a registry of tasks, it is used to build dependencies

    # PIS stage of the DAG.
    # This stage will run the PIS steps in parallel by replicating the following pattern for each:
    # c. Check if the step must be run, if not, jump to j
    # u. Upload the step configuration to GCS
    # r. Run the step in a Compute Engine VM, waiting for it to produce an exit code
    # d. Delete the VM
    # j. Join the parallel branches
    @task_group(group_id="pis_stage")
    def pis_stage() -> None:
        for step_name in config.pis_step_list:

            @task_group(group_id=f"pis_{step_name}")
            def pis_step(step_name: str) -> None:
                config_gcs_url = config.pis_config_gcs_url(step_name)
                vm_name = f"uo-pis-{clean_name(step_name)}-{{{{ run_id | strhash }}}}"
                vm_env = {
                    "PIS_STEP": step_name,
                    "PIS_CONFIG_FILE": "/config.yaml",
                    "PIS_POOL": config.pis_pool,
                }
                labels = Labels(
                    {
                        "tool": "pis",
                        "step": step_name,
                        "product": "ppp" if config.is_ppp else "platform",
                    }
                )

                c = PISDiffComputeOperator(
                    task_id=f"diff_{step_name}",
                    step_name=step_name,
                    local_config=config.pis_config,
                    remote_config_url=config_gcs_url,
                )

                u = UploadStringOperator(
                    task_id=f"upload_config_{step_name}",
                    contents=to_yaml(config.pis_config),
                    dst=config_gcs_url,
                )

                r = ComputeEngineRunContainerizedWorkloadSensor(
                    task_id=f"run_{step_name}",
                    instance_name=vm_name,
                    labels=labels,
                    container_image=config.pis_image,
                    container_env=vm_env,
                    container_service_account=config.service_account,
                    container_scopes=config.service_account_scopes,
                    container_files={config_gcs_url: "/config.yaml"},
                    work_disk_size_gb=config.pis_disk_size,
                    deferrable=True,
                )

                d = ComputeEngineDeleteInstanceOperator(
                    task_id=f"delete_vm_{step_name}",
                    project_id=GCP_PROJECT_PLATFORM,
                    zone=GCP_ZONE,
                    resource_id=vm_name,
                )

                j = EmptyOperator(
                    task_id=f"join_{step_name}",
                    trigger_rule="none_failed_min_one_success",
                )

                # add the run task to the step registry
                steps[f"pis_{step_name}"] = j
                # here we define the task dependencies for both branches
                c >> Label("invalid previous run") >> u >> r >> d >> j
                c >> Label("valid previous run exists, skip run") >> j

            pis_step(step_name)

    pis_stage()

    # ETL stage of the DAG.
    # p. Prepare the Dataproc cluster
    #   c. Creation
    #   uc. Upload the ETL configuration to GCS
    #   uj. Upload the ETL JAR to GCS
    # r. The ETL steps are run in parallel, as soon as their prerequisites are met.
    #    The required PIS and ETL run tasks are added as upstream dependencies to each step task.
    # d. Delete the Dataproc cluster
    cluster_name = "uo-etl-{{ run_id | strhash }}"
    labels_etl = Labels({"tool": "etl"})

    @task_group(group_id=f"etl_cluster_prepare")
    def etl_cluster_prepare() -> None:
        c = DataprocCreateClusterOperator(
            task_id="cluster_create",
            cluster_name=cluster_name,
            labels=labels_etl,
        )
        uc = UploadStringOperator(
            task_id=f"upload_config",
            contents=to_hocon(config.etl_config),
            dst=config.etl_config_gcs_uri,
        )
        uj = UploadRemoteFileOperator(
            task_id=f"upload_jar",
            src=config.etl_jar_origin_url,
            dst=config.etl_jar_gcs_uri,
        )
        c >> uc >> uj

    p = etl_cluster_prepare()

    @task_group(group_id="etl_stage")
    def etl_stage() -> None:
        for step in config.etl_step_list:
            step_name = step["name"]
            labels_etl_step = labels_etl.clone({"step": step_name})

            pis_dependencies = [p for p in step.get("depends_on", []) if "pis_" in p]
            etl_dependencies = [p for p in step.get("depends_on", []) if "etl_" in p]

            r = DataprocSubmitJobOperator(
                task_id=f"run_{step_name}",
                step_name=step_name,
                cluster_name=cluster_name,
                jar_file_uri=config.etl_jar_gcs_uri,
                config_file_uri=config.etl_config_gcs_uri,
                labels=labels_etl_step,
            )
            r.set_upstream([steps[dep] for dep in pis_dependencies])
            r.set_upstream([steps[dep] for dep in etl_dependencies])
            steps[f"etl_{step_name}"] = r

    r = etl_stage()

    d = DataprocDeleteClusterOperator(
        task_id="cluster_delete",
        project_id=GCP_PROJECT_PLATFORM,
        region=GCP_REGION,
        cluster_name=cluster_name,
        trigger_rule="all_success",
    )

    p >> r >> d
