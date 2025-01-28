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
from airflow.utils.trigger_rule import TriggerRule

from ot_orchestration.dags.config.unified_pipeline import (
    UnifiedPipelineConfig,
)
from ot_orchestration.operators.batch.vep import VepAnnotateOperator
from ot_orchestration.operators.dataproc import (
    PlatformETLCreateClusterOperator,
    PlatformETLSubmitJobOperator,
)
from ot_orchestration.operators.gce import ComputeEngineRunContainerizedWorkloadSensor
from ot_orchestration.operators.gcs import CopyBlobOperator, UploadStringOperator
from ot_orchestration.operators.unified_pipeline import PISDiffComputeOperator
from ot_orchestration.utils import (
    create_cluster_name,
    create_name,
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
from ot_orchestration.utils.dataproc import (
    create_cluster,
    delete_cluster,
    submit_gentropy_step,
)
from ot_orchestration.utils.labels import StepLabels

with DAG(
    default_args=shared_dag_args,
    **unified_pipeline_dag_kwargs,
    params={
        "run_label": Param(
            default=f"up-{datetime.now().strftime('%Y%m%d-%H%M')}",
            description="""A label with key 'run' and the contents of this parameter
                           will be added to any infrastructure resources that this
                           pipeline creates in Google Cloud.""",
        ),
    },
) as dag:
    config = UnifiedPipelineConfig()
    steps = {}  # this is a registry of tasks, it is used to build dependencies

    # ==============================================================================================
    # PIS stage of the DAG
    #
    # c. Check if the step must be run, if not, jump to j.
    # u. Upload the step configuration to GCS.
    # r. Run the step in a Compute Engine VM, waiting for it to produce an exit code.
    # j. Join the parallel branches.
    # d. Delete the VM.
    # ==============================================================================================
    @task_group(group_id="pis_stage")
    def pis_stage() -> None:
        for step_name in config.pis_step_list:
            # skip ppp steps if the pipeline is not running in ppp mode
            if not config.is_ppp and step_name in config.ppp_steps:
                continue

            @task_group(group_id=step_name)
            def pis_step(step_name: str) -> None:
                config_uri = config.pis_config_uri(step_name)
                labels = StepLabels("pis", step_name, config.is_ppp)
                vm_name = create_name(step_name)

                c = PISDiffComputeOperator(
                    task_id=f"diff_{step_name}",
                    step_name=step_name,
                    local_config=config.pis_config,
                    remote_config_uri=config_uri,
                )

                u = UploadStringOperator(
                    task_id=f"upload_config_{step_name}",
                    contents=to_yaml(config.pis_config),
                    dst_uri=config_uri,
                    overwrite=True,
                )

                r = ComputeEngineRunContainerizedWorkloadSensor(
                    task_id=f"run_{step_name}",
                    instance_name=vm_name,
                    labels=labels,
                    container_image=config.pis_image,
                    container_env=config.pis_env_vars(step_name),
                    container_service_account=config.service_account,
                    container_scopes=config.service_account_scopes,
                    container_files={config_uri: "/config.yaml"},
                    work_disk_size_gb=config.pis_disk_size,
                    deferrable=True,
                )

                j = EmptyOperator(
                    task_id=f"join_{step_name}",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )
                # add the final step to the step registry
                steps[step_name] = j

                d = ComputeEngineDeleteInstanceOperator(
                    task_id=f"delete_vm_{step_name}",
                    project_id=GCP_PROJECT_PLATFORM,
                    zone=GCP_ZONE,
                    resource_id=vm_name,
                    trigger_rule=TriggerRule.NONE_SKIPPED,
                )

                # here we define the task dependencies for both branches
                chain(c, Label("invalid previous run"), u, r, (j, d))
                chain(c, Label("valid previous run exists, skip run"), j)

            pis_step(step_name)

    pis_stage()

    # ==============================================================================================
    # ONTOFORM stage of the DAG
    #
    # r. Run the step in a Compute Engine VM, waiting for it to produce an exit code.
    # d. Delete the VM.
    # ==============================================================================================
    if len(config.ontoform_step_list):

        @task_group(group_id="ontoform_stage")
        def ontoform_stage() -> None:
            for step_name in config.ontoform_step_list:

                @task_group(group_id=step_name)
                def ontoform_step(step_name: str) -> None:
                    labels = StepLabels("ontoform", step_name, config.is_ppp)
                    vm_name = create_name(step_name)

                    r = ComputeEngineRunContainerizedWorkloadSensor(
                        task_id=f"run_{step_name}",
                        instance_name=vm_name,
                        labels=labels,
                        container_image=config.ontoform_image,
                        container_service_account=config.service_account,
                        container_scopes=config.service_account_scopes,
                        container_args=config.ontoform_args(step_name),
                        machine_type=config.ontoform_machine_type,
                        deferrable=True,
                    )

                    d = ComputeEngineDeleteInstanceOperator(
                        task_id=f"delete_vm_{step_name}",
                        project_id=GCP_PROJECT_PLATFORM,
                        zone=GCP_ZONE,
                        resource_id=vm_name,
                        trigger_rule=TriggerRule.NONE_SKIPPED,
                    )

                    steps[step_name] = r
                    chain(r, d)

                ontoform_step(step_name)

        ontoform_stage()

    # ==============================================================================================
    # ETL stage of the DAG
    #
    # p. Prepare the ETL Dataproc cluster.
    #   c. Create the cluster.
    #   uc. Upload the ETL configuration to GCS.
    #   cj. Copy the ETL JAR.
    # r. The ETL steps are run in parallel, as their prerequisites are met.
    # d. Delete the Dataproc cluster.
    # ==============================================================================================
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
            dst_uri=config.etl_config_uri,
            overwrite=True,
        )
        cj = CopyBlobOperator(
            task_id=f"upload_jar",
            src_uri=config.etl_jar_origin_uri,
            dst_uri=config.etl_jar_uri,
            overwrite=True,
        )

        chain(c, uc, cj)

    p = etl_cluster_prepare()

    @task_group(group_id="etl_stage")
    def etl_stage() -> None:
        for step_name in config.etl_step_list:
            # skip ppp steps if the pipeline is not running in ppp mode
            if not config.is_ppp and step_name in config.ppp_steps:
                continue

            labels = StepLabels("etl", step_name, config.is_ppp)

            r = PlatformETLSubmitJobOperator(
                task_id=f"run_{step_name}",
                step_name=step_name.replace("etl_", ""),  # remove the etl prefix
                cluster_name=etl_cluster_name,
                jar_uri=config.etl_jar_uri,
                config_uri=config.etl_config_uri,
                labels=labels,
            )
            steps[step_name] = r

    s = etl_stage()

    d = DataprocDeleteClusterOperator(
        task_id="etl_cluster_delete",
        project_id=GCP_PROJECT_PLATFORM,
        region=GCP_REGION,
        cluster_name=etl_cluster_name,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    chain(p, s, d)

    # ==============================================================================================
    # Gentropy stage of the DAG.
    #
    # c. Prepare the Gentropy Dataproc cluster.
    # r. The Gentropy steps are run in parallel, as their prerequisites are met.
    #       There are different types of Gentropy steps. We match special cases by
    #       name in the config to define custom tasks for them. Most are dataproc
    #       jobs that require a cluster, and those are the default case in the match.
    #       determine if the step is special and if so, we p
    # d. Delete the Dataproc cluster
    #
    # Note: labels are generated but not used yet, pending refactor of cluster
    #       management functions into operators.
    # ==============================================================================================
    if len(config.gentropy_step_list):
        gentropy_cluster_name = create_cluster_name("gentropy")
        clusterless_steps = []

        c = create_cluster(
            cluster_name=gentropy_cluster_name,
            project_id=GCP_PROJECT_PLATFORM,
            **config.gentropy_dataproc_cluster_settings,
            idle_delete_ttl=90 * 60,
        )

        d = delete_cluster(
            gentropy_cluster_name,
            project_id=GCP_PROJECT_PLATFORM,
        )

        @task_group(group_id="gentropy_stage")
        def gentropy_stage() -> None:
            for step_name in config.gentropy_step_list:
                step_config = config.gentropy_step(step_name)
                labels = StepLabels("gentropy", step_name, config.is_ppp)

                match step_name:
                    case "gentropy_variant_annotation":
                        r = VepAnnotateOperator(
                            job_name=create_name("variant_annotation"),
                            task_id=f"run_{step_name}",
                            project_id=GCP_PROJECT_PLATFORM,
                            **step_config["params"],
                            google_batch=step_config["google_batch"],
                            labels=labels,
                        )
                        clusterless_steps.append(r)
                    case _:
                        r = submit_gentropy_step(
                            cluster_name=gentropy_cluster_name,
                            step_name=step_name,
                            project_id=GCP_PROJECT_PLATFORM,
                            python_main_module=config.gentropy_python_main_module,
                            params=step_config["params"],
                            labels=labels,
                        )

                steps[step_name] = r
                if r not in clusterless_steps:
                    chain(c, r, d)

        r = gentropy_stage()

    # ==============================================================================================
    # After creating all the tasks, we tie them together by creating dependencies.
    for step_name in steps:
        if step_config := config.steps.get(step_name):
            for dep in step_config.get("depends_on", []):
                steps[step_name].set_upstream(steps[dep])
            if config.is_ppp:
                for ppp_dep in step_config.get("depends_on_ppp", []):
                    steps[step_name].set_upstream(steps[ppp_dep])

if __name__ == "__main__":
    dag.test()
