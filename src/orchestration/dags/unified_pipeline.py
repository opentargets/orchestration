"""DAG for the Open Targets unified pipeline."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from airflow.decorators.task_group import task_group
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.taskmixin import DAGNode
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.compute import ComputeEngineDeleteInstanceOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from orchestration.dags.config.unified_pipeline import UnifiedPipelineConfig
from orchestration.models.batch import BatchIndexOperatorSpec, BatchJobOperatorSpec
from orchestration.models.pts_step import PTSDataprocStep, pts_step_from_config
from orchestration.operators.batch import BatchIndexOperator, BatchJobOperator
from orchestration.operators.dataproc import (
    CreateClusterOperator,
    DeleteClusterOperator,
    ETLJobBuilder,
    GentropyJobBuilder,
    SubmitJobOperator,
)
from orchestration.operators.diff import DiffOperator
from orchestration.operators.differs.config_differ import ConfigDiffer
from orchestration.operators.differs.manifest_artifact_differ import ManifestArtifactDiffer
from orchestration.operators.differs.spark_job_differ import SparkJobDiffer
from orchestration.operators.gce import ComputeEngineRunContainerizedWorkloadSensor, DeleteInstanceOperator
from orchestration.operators.gcs import CopyBlobOperator, UploadFileOperator, UploadStringOperator
from orchestration.utils import resource_name, strhash, to_hocon, to_yaml
from orchestration.utils.common import GCP_PROJECT_PLATFORM, GCP_ZONE, shared_dag_args
from orchestration.utils.labels import Labels

if TYPE_CHECKING:
    from orchestration.operators.differs.differ import Differ

with DAG(
    dag_id="unified_pipeline",
    description="Open Targets unified data pipeline",
    default_args=shared_dag_args,
    default_view="grid",
    catchup=False,
    schedule=None,
    user_defined_filters={"strhash": strhash},
    params={
        "run_label": Param(
            default=f"up-{datetime.now().strftime('%Y%m%d-%H%M')}",
            description="""A label with key 'run' and the contents of this parameter
                           will be added to any infrastructure resources that this
                           pipeline creates in Google Cloud.""",
        ),
    },
) as dag:
    logger = logging.getLogger(__name__)
    config = UnifiedPipelineConfig()
    steps: dict[str, dict[str, DAGNode]] = {}  # this is a registry of tasks, it is used to build dependencies

    # ==============================================================================================
    # PIS stage of the DAG —  two paths
    #
    # 1.
    #   d. Diff   — the state and decide the step needs to run.
    #   u. Upload — the step's config to GCS.
    #   r. Run    — the step in a GCE VM, and wait until it finishes.
    #   t. Delete — the VM.
    #   e. End    — the step (does nothing).
    #
    # 2.
    #   d. Diff   — the state and decide the step does not need to run.
    #   e. End    — the step (does nothing).
    # ==============================================================================================
    def pis_stage() -> None:
        for step_name in config.steps("pis_"):

            @task_group(group_id=step_name)
            def pis_step(step_name: str) -> None:
                config_uri = config.config_uri(step_name)
                labels = Labels({"tool": "pis", "step": step_name}, is_ppp=config.is_ppp)
                vm_name = resource_name(step_name)

                d = DiffOperator(
                    task_id=f"diff_{step_name}",
                    step_name=step_name,
                    differs=[
                        ConfigDiffer(),
                        ManifestArtifactDiffer(),
                    ],
                    config=config,
                )

                u = UploadStringOperator(
                    task_id=f"upload_config_{step_name}",
                    contents=to_yaml(config.step_config(step_name)),
                    dst_uri=config_uri,
                    overwrite=True,
                )

                r = ComputeEngineRunContainerizedWorkloadSensor(
                    task_id=f"run_{step_name}",
                    instance_name=vm_name,
                    labels=labels,
                    container_image=config.pis_image,
                    container_env=config.pis_env_vars(step_name),
                    container_scopes=config.service_account_extra_scopes,
                    container_files={config_uri: "/config.yaml"},
                    work_disk_size_gb=config.pis_disk_size,
                    deferrable=True,
                )

                t = DeleteInstanceOperator(
                    task_id=f"delete_vm_{step_name}",
                    resource_id=vm_name,
                )

                e = EmptyOperator(
                    task_id=f"end_{step_name}",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )

                chain(d, Label("differences found, run step"), u, r, (t, e))
                chain(d, Label("no differences found, skip step"), e)
                steps[step_name] = {"start": d, "end": e}

            pis_step(step_name)

    pis_stage()

    # ==============================================================================================
    # PTS stage of the DAG — two paths
    #
    # 1.
    #   d. Diff   — the state and decide the step needs to run.
    #   u. Upload — the step's config to GCS.
    #   r. Run    — the step in a GCE VM, and wait until it finishes.
    #   t. Delete — the VM.
    #   e. End    — the step (does nothing).
    #
    # 2.
    #   d. Diff   — the state and decide the step does not need to run.
    #   e. End    — the step (does nothing).
    # ==============================================================================================
    def pts_stage() -> None:
        pts_clusters = {}  # map of cluster_name to a list of step_names
        for step_name in config.steps("pts_"):

            @task_group(group_id=step_name)
            def pts_step(step_name: str) -> None:
                s = pts_step_from_config(step_name, config)
                step_definition = config.step_definition(step_name)

                config_uri = config.config_uri(step_name)
                labels = Labels({"tool": "pts", "step": step_name}, is_ppp=config.is_ppp)
                vm_name = resource_name(step_name)

                d = DiffOperator(
                    task_id=f"diff_{step_name}",
                    step_name=step_name,
                    config=config,
                    differs=[
                        ConfigDiffer(),
                        ManifestArtifactDiffer(),
                    ],
                )

                u = UploadStringOperator(
                    task_id=f"upload_config_{step_name}",
                    contents=to_yaml(config.step_config(step_name)),
                    dst_uri=config_uri,
                    overwrite=True,
                )

                chain(d, Label("differences found, run step"), u)

                if s.is_gce:
                    r = ComputeEngineRunContainerizedWorkloadSensor(
                        task_id=f"run_{step_name}",
                        instance_name=vm_name,
                        labels=labels,
                        container_image=config.pts_image,
                        container_env=config.pts_env_vars(step_name),
                        container_scopes=config.service_account_extra_scopes,
                        container_files={config_uri: "/config.yaml"},
                        container_secret_files=step_definition.get("gce_secret_files"),
                        work_disk_size_gb=config.pts_disk_size,
                        machine_type=config.pts_machine_type,
                        deferrable=True,
                    )

                    t = ComputeEngineDeleteInstanceOperator(
                        task_id=f"delete_vm_{step_name}",
                        project_id=GCP_PROJECT_PLATFORM,
                        zone=GCP_ZONE,
                        resource_id=vm_name,
                    )

                    chain(u, Label("gce pts step"), r, t)

                elif s.is_dataproc:
                    s = cast(PTSDataprocStep, s)
                    cluster_name = s.cluster_definition.cluster_type

                    u2 = UploadFileOperator(
                        task_id=f"upload_entrypoint_{step_name}",
                        project_id=GCP_PROJECT_PLATFORM,
                        src_path=s.dataproc_script_run_source,
                        dst_uri=s.dataproc_script_run_uri,
                    )

                    c = CreateClusterOperator(
                        task_id=f"create_cluster_{cluster_name}",
                        cluster_name=s.cluster_definition.cluster_name,
                        cluster_config=s.cluster_definition.cluster_config,
                        labels=labels,
                    )

                    r = SubmitJobOperator(
                        task_id=f"run_{step_name}",
                        cluster_name=s.cluster_definition.cluster_name,
                        step_name=step_name,
                        py_spark_job=s.build_job(),
                        labels=labels,
                    )

                    steps_in_cluster = pts_clusters.get(cluster_name, [])
                    pts_clusters[cluster_name] = [*steps_in_cluster, step_name]
                    chain(u, Label("dataproc pts step"), u2, c, r)

                e = EmptyOperator(
                    task_id=f"end_{step_name}",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )

                if s.is_gce:
                    chain(t, e)
                elif s.is_dataproc:
                    chain(r, e)
                chain(d, Label("no differences found, skip step"), e)
                steps[step_name] = {"start": d, "end": e}

            pts_step(step_name)

        # delete a cluster after its steps have run
        for cluster_name, steps_in_cluster in pts_clusters.items():
            x = DeleteClusterOperator(
                task_id=f"cluster_delete_{cluster_name}",
                cluster_name=cluster_name,
                trigger_rule=TriggerRule.ALL_SUCCESS,
            )
            for step_name in steps_in_cluster:
                step = steps[step_name]["end"]
                x.set_upstream(step)

    pts_stage()

    # ==============================================================================================
    # ETL stage of the DAG — two paths
    #
    # 1.
    #   d.  Diff           — the state and decide the step needs to run.
    #   uc. Upload Config  — to GCS.
    #   uj. Upload JAR     — to GCS.
    #   c.  Create         — cluster in Dataproc, if it does not exist.
    #   r.  Run            — the step on the cluster from task `s`.
    #   e.  End            — the step (does nothing).
    #
    # 2.
    #   d. Diff            — the state and decide the step does not need to run.
    #   e. End             — the step (does nothing).
    #
    # After all steps have run:
    #   x. Delete          — the cluster, if it was created.
    # ==============================================================================================
    def etl_stage() -> None:
        cluster_name = "etl"

        for step_name in config.steps("etl_"):

            @task_group(group_id=step_name)
            def etl_step(step_name: str) -> None:
                config_uri = config.config_uri(step_name)
                jar_uri = config.jar_uri(step_name)
                labels = Labels({"tool": "etl"}, is_ppp=config.is_ppp)

                d = DiffOperator(
                    task_id=f"diff_{step_name}",
                    step_name=step_name,
                    config=config,
                    differs=[
                        ConfigDiffer(),
                        SparkJobDiffer(),
                    ],
                )

                uc = UploadStringOperator(
                    task_id=f"upload_config_{step_name}",
                    contents=to_hocon(config.step_config(step_name)),
                    dst_uri=config_uri,
                    overwrite=True,
                )

                uj = CopyBlobOperator(
                    task_id="upload_jar",
                    src_uri=config.etl_jar_origin_uri,
                    dst_uri=jar_uri,
                    overwrite=True,
                )

                cluster_definition = config.step_cluster_definition(step_name)
                assert cluster_definition is not None
                cluster_name = resource_name(cluster_definition.cluster_type)
                num_partitions = str(config.step_definition(step_name).get("num_partitions", config.num_partitions))

                c = CreateClusterOperator(
                    task_id="cluster_create_etl",
                    cluster_name=cluster_name,
                    cluster_config=cluster_definition.cluster_config,
                    labels=labels,
                )

                labels["step"] = step_name

                r = SubmitJobOperator(
                    task_id=f"run_{step_name}",
                    cluster_name=cluster_name,
                    step_name=step_name,
                    spark_job=ETLJobBuilder(
                        jar_uri=jar_uri,
                        config_uri=config_uri,
                        args=[step_name.split("_", 1)[-1]],
                        properties=config.step_job_properties(step_name),
                        template_context={
                            "config_filename": config_uri.rsplit("/")[-1],
                            "num_partitions": num_partitions,
                        },
                    ).build(),
                    labels=labels,
                )

                e = EmptyOperator(
                    task_id=f"end_{step_name}",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )

                chain(d, Label("differences found, run step"), uc, uj, c, r, e)
                chain(d, Label("no differences found, skip step"), e)
                steps[step_name] = {"start": d, "end": e}

            etl_step(step_name)

        if etl_step_ends := [step["end"] for step_name, step in steps.items() if step_name.startswith("etl_")]:
            x = DeleteClusterOperator(
                task_id="cluster_delete_etl",
                cluster_name=resource_name(cluster_name),
            )

            for end in etl_step_ends:
                x.set_upstream(end)

    etl_stage()

    # ==============================================================================================
    # GENTROPY stage of the DAG
    #
    # This initial part is a temporary definition of what are GENTROPY's steps output paths and the
    # differs each step uses to check if it needs to run or not.
    #
    # We can get rid of this once we work on https://github.com/opentargets/issues/issues/3652
    # ==============================================================================================
    def gsp(step_name: str, param_name: str) -> dict[str, Any]:
        step_config = config.step_specific_config(step_name)
        if "google_batch_index_specs" in step_config:
            batch_index_specs = step_config.get("google_batch_index_specs", {})
            manifest_generator_specs = batch_index_specs.get("manifest_generator_specs", {})
            step_params = manifest_generator_specs.get("options", {})
        else:
            step_params = step_config.get("params", {})
        return step_params.get(param_name)

    def gentropy_step_differs(step_name: str) -> list[Differ]:
        gentropy_step_outputs: dict[str, dict[str, Any]] = {
            "gentropy_biosample": {
                "step.biosample_index_path": gsp("gentropy_biosample", "step.biosample_index_path"),
            },
            "gentropy_study": {
                "step.valid_study_index_path": gsp("gentropy_study", "step.valid_study_index_path"),
                "step.invalid_study_index_path": gsp("gentropy_study", "step.invalid_study_index_path"),
            },
            "gentropy_credible_set": {
                "step.valid_study_locus_path": gsp("gentropy_credible_set", "step.valid_study_locus_path"),
                "step.invalid_study_locus_path": gsp("gentropy_credible_set", "step.invalid_study_locus_path"),
            },
            "gentropy_colocalisation": {
                "step.coloc_path": gsp("gentropy_colocalisation", "step.coloc_path"),
            },
            "gentropy_variant_partition": {
                "step.output_path": gsp("gentropy_variant_partition", "step.output_path"),
            },
            "gentropy_variant_annotation": {
                "vep_output_path": gsp("gentropy_variant_annotation", "vep_output_path"),
            },
            "gentropy_variant": {
                "step.variant_index_path": gsp("gentropy_variant", "step.variant_index_path"),
            },
            "gentropy_l2g_feature_matrix": {
                "step.feature_matrix_path": gsp("gentropy_l2g_feature_matrix", "step.feature_matrix_path"),
            },
            "gentropy_l2g_training": {
                "step.model_path": gsp("gentropy_l2g_training", "step.model_path"),
            },
            "gentropy_l2g_prediction": {
                "step.predictions_path": gsp("gentropy_l2g_prediction", "step.predictions_path"),
            },
            "gentropy_l2g_evidence": {
                "step.evidence_output_path": gsp("gentropy_l2g_evidence", "step.evidence_output_path"),
            },
            "gentropy_enhancer_to_gene": {
                "step.valid_output_path": gsp("gentropy_enhancer_to_gene", "step.valid_output_path"),
                "step.invalid_output_path": gsp("gentropy_enhancer_to_gene", "step.invalid_output_path"),
            },
        }

        default_differs = [
            ConfigDiffer(),
            SparkJobDiffer(outputs=gentropy_step_outputs.get(step_name)),
        ]

        gentropy_step_differs: dict[str, list[Differ]] = {
            "gentropy_variant_annotation": [
                ConfigDiffer(),
                # TODO: What else can we check in here?
            ],
        }

        return gentropy_step_differs.get(step_name, default_differs)

    # ==============================================================================================
    # GENTROPY stage of the DAG — two paths
    #
    # 1.
    #   d. Diff   — the state and decide the step needs to run.
    #   u. Upload — the step's config to GCS.
    #   c. create — cluster cluster in dataproc, or do nothing if the step does not run on a cluster.
    #   r.  Run   — the step on the cluster from task `c`, or using custom operators.
    #   e.  End            — the step (does nothing).
    #
    # 2.
    #   d. Diff            — the state and decide the step does not need to run.
    #   e. End             — the step (does nothing).
    #
    # After all steps assigned to each cluster have run:
    #   x. Delete          — the cluster for those steps.
    # ==============================================================================================
    def gentropy_stage() -> None:
        gentropy_clusters = {}  # map of cluster_name to a list of step_names
        gentropy_steps = {}  # map of step_name to airflow task
        for step_name in config.steps("gentropy_"):

            @task_group(group_id=step_name)
            def gentropy_step(step_name: str) -> None:
                config_uri = config.config_uri(step_name)
                labels = Labels({"tool": "gentropy", "step": step_name}, is_ppp=config.is_ppp)

                d = DiffOperator(
                    task_id=f"diff_{step_name}",
                    step_name=step_name,
                    config=config,
                    differs=gentropy_step_differs(step_name),
                )

                u = UploadStringOperator(
                    task_id=f"upload_config_{step_name}",
                    contents=to_yaml(config.step_config(step_name)),
                    dst_uri=config_uri,
                    overwrite=True,
                )

                # find what cluster type the step runs on, if any
                cluster_definition = config.step_cluster_definition(step_name)
                if cluster_definition:
                    cluster_name = cluster_definition.cluster_type

                    c = CreateClusterOperator(
                        task_id=f"cluster_create_{cluster_name}",
                        cluster_name=resource_name(cluster_name),
                        cluster_config=cluster_definition.cluster_config,
                        labels=labels,
                    )
                    # add the run task to the proper cluster list in the gentropy_clusters dict
                    steps_in_cluster = gentropy_clusters.get(cluster_name, [])
                    gentropy_clusters[cluster_name] = [*steps_in_cluster, step_name]

                    r = SubmitJobOperator(
                        task_id=f"run_{step_name}",
                        cluster_name=resource_name(cluster_name),
                        step_name=step_name,
                        py_spark_job=GentropyJobBuilder(
                            main_python_file_uri=config.gentropy_main_python_file_uri,
                            params=config.step_specific_config(step_name).get("params"),
                            properties=config.step_job_properties(step_name),
                        ).build(),
                        labels=labels,
                    )
                    gentropy_steps[step_name] = r
                else:
                    c = EmptyOperator(task_id="skip_cluster")

                    @task_group(group_id=step_name + "_batch_jobs")
                    def batch_jobs(step_name: str) -> None:
                        i = BatchIndexOperator(
                            task_id=f"generate_manifest_{step_name}",
                            batch_index_specs=BatchIndexOperatorSpec(
                                **config.step_specific_config(step_name).get("google_batch_index_specs", {})
                            ),
                        )
                        b = BatchJobOperator.partial(
                            job_name=resource_name(step_name),
                            task_id=f"run_{step_name}",
                            batch_job_spec=BatchJobOperatorSpec(
                                **config.step_specific_config(step_name).get("google_batch", {})
                            ),
                            project_id=GCP_PROJECT_PLATFORM,
                            labels=labels,
                        ).expand(batch_index_row=i.output)
                        chain(i, b)

                    r = batch_jobs(step_name)

                e = EmptyOperator(
                    task_id=f"end_{step_name}",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )

                chain(d, Label("differences found, run step"), u, c, r, e)
                chain(d, Label("no differences found, skip step"), e)
                steps[step_name] = {"start": d, "end": e}

            gentropy_step(step_name)

        # delete a cluster after its steps have run
        if gentropy_clusters:
            for cluster_name, steps_in_cluster in gentropy_clusters.items():
                x = DeleteClusterOperator(
                    task_id=f"cluster_delete_{cluster_name}",
                    cluster_name=resource_name(cluster_name),
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                )
                for step_name in steps_in_cluster:
                    step = steps[step_name]["end"]
                    x.set_upstream(step)

    gentropy_stage()

    # ==============================================================================================
    # After creating all the tasks, we tie them together by creating dependencies.
    for step_name, step_tasks in steps.items():
        step_definition = config.step_definition(step_name) or {}
        for dep in step_definition.get("depends_on", []):
            step_tasks["start"].set_upstream(steps[dep]["end"])
        if config.is_ppp:
            for ppp_dep in step_definition.get("depends_on_ppp", []):
                step_tasks["start"].set_upstream(steps[ppp_dep]["end"])

if __name__ == "__main__":
    dag.test()
