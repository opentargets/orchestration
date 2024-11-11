"""DAG for the Open Targets unified pipeline."""

from datetime import datetime

from airflow.decorators.task_group import task_group
from airflow.models.baseoperator import chain
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

from ot_orchestration.dags.config.unified_pipeline import PlatformConfig
from ot_orchestration.operators.dataproc import (
    PlatformETLCreateClusterOperator,
    PlatformETLSubmitJobOperator,
)
from ot_orchestration.operators.gce import ComputeEngineRunContainerizedWorkloadSensor
from ot_orchestration.operators.gcs import (
    UploadRemoteFileOperator,
    UploadStringOperator,
)
from ot_orchestration.operators.unified_pipeline import PISDiffComputeOperator
from ot_orchestration.utils import (
    create_cluster_name,
    create_vm_name,
    to_hocon,
    to_yaml,
)
from ot_orchestration.utils.common import (
    GCP_PROJECT_PLATFORM,
    GCP_REGION,
    GCP_ZONE,
    shared_dag_args,
    unified_pipeline_dag_kwargs,
)
from ot_orchestration.utils.labels import StepLabels

with DAG(
    default_args=shared_dag_args,
    **unified_pipeline_dag_kwargs,
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

            @task_group(group_id=step_name)
            def pis_step(step_name: str) -> None:
                config_gcs_url = config.pis_config_gcs_url(step_name)
                labels = StepLabels("pis", step_name, config.is_ppp)
                vm_name = create_vm_name(step_name)

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
                steps[step_name] = j
                # here we define the task dependencies for both branches
                chain(c, Label("invalid previous run"), u, r, d, j)
                chain(c, Label("valid previous run exists, skip run"), j)

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
    etl_cluster_name = create_cluster_name("etl")

    @task_group(group_id=f"etl_cluster_prepare")
    def etl_cluster_prepare() -> None:
        labels = StepLabels("etl", is_ppp=config.is_ppp)

        c = PlatformETLCreateClusterOperator(
            task_id="cluster_create",
            cluster_name=etl_cluster_name,
            labels=labels,
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
        chain(c, uc, uj)

    p = etl_cluster_prepare()

    @task_group(group_id="etl_stage")
    def etl_stage() -> None:
        for step_name in config.etl_step_list:
            labels = StepLabels("etl", step_name, config.is_ppp)

            r = PlatformETLSubmitJobOperator(
                task_id=f"run_{step_name}",
                step_name=step_name.replace("etl_", ""),  # remove the etl prefix
                cluster_name=etl_cluster_name,
                jar_file_uri=config.etl_jar_gcs_uri,
                config_file_uri=config.etl_config_gcs_uri,
                labels=labels,
            )
            steps[step_name] = r

    r = etl_stage()

    d = DataprocDeleteClusterOperator(
        task_id="etl_cluster_delete",
        project_id=GCP_PROJECT_PLATFORM,
        region=GCP_REGION,
        cluster_name=etl_cluster_name,
        trigger_rule="all_success",
    )

    chain(p, r, d)

if __name__ == "__main__":
    dag.test()
