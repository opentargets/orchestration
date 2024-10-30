"""Test DAG to prototype data transfer."""

from __future__ import annotations

import time
from pathlib import Path

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup

from ot_orchestration.operators.batch.vep import (
    ConvertVariantsToVcfOperator,
    VepAnnotateOperator,
)
from ot_orchestration.utils import (
    GCSPath,
    chain_dependencies,
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.batch import (
    create_batch_job,
    create_task_commands,
    create_task_env,
    create_task_spec,
)
from ot_orchestration.utils.common import (
    GCP_PROJECT_GENETICS,
    GCP_REGION,
    shared_dag_args,
    shared_dag_kwargs,
)
from ot_orchestration.utils.dataproc import (
    generate_dataproc_task_chain,
    submit_gentropy_step,
)

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "genetics_etl.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
nodes = config["nodes"]
node_map = {}


# FIXME: eventually this task group should have 2 steps only
# - 1. transform variant sources to vcf files, collect and partition them by chunk size - should be done by a single gentropy step rather then
# multiple tasks in the DAG (pending)
# - 2. list new chunk vcf files and annotate them - batch job


# This operator meant to fail the DAG if the release folder exists:
with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    # Compiling tasks for moving data to the right place:
    with TaskGroup(group_id="data_transfer") as data_transfer:
        # Defining the tasks to execute in the task group:
        l2g_gold_standard_path = GCSPath(config["l2g_gold_standard_path"])
        release_dir = GCSPath(config["release_dir"])
        # Files to move:
        DATA_TO_MOVE = {
            # L2G gold standard:
            "l2g_gold_standard": {
                "source_bucket": l2g_gold_standard_path.bucket,
                "source_object": l2g_gold_standard_path.path,
                "destination_bucket": release_dir.bucket,
                "destination_object": f"{release_dir.path}/locus_to_gene_gold_standard.json",
            },
        }
        [
            GCSToGCSOperator(
                task_id=f"move_{data_name}",
                source_bucket=data["source_bucket"],
                source_object=data["source_object"],
                destination_bucket=data["destination_bucket"],
                destination_object=data["destination_object"],
            )
            for data_name, data in DATA_TO_MOVE.items()
        ]

    with TaskGroup(group_id="genetics_etl") as genetics_etl:
        # ===== Variant annotation task group ======
        with TaskGroup(group_id="variant_annotation") as variant_annotation:
            variant_annotation_task_group_config = find_node_in_config(
                nodes, "variant_annotation"
            )

            # ===== Convert variant datasource to vcf file step ======
            task_config = find_node_in_config(
                variant_annotation_task_group_config["nodes"],
                node_id="variant_to_vcf",
            )
            google_batch_config = task_config["google_batch"]
            commands = create_task_commands(
                commands=google_batch_config["commands"],
                params=task_config["params"],
            )
            task = create_task_spec(
                image=google_batch_config["image"],
                commands=commands,
                resource_specs=google_batch_config["resource_specs"],
                task_specs=google_batch_config["task_specs"],
                entrypoint=google_batch_config["entrypoint"],
            )
            environment = google_batch_config["environment"]
            batch_job = create_batch_job(
                task=task,
                task_env=create_task_env(environment),
                policy_specs=google_batch_config["policy_specs"],
            )
            variant_to_vcf = CloudBatchSubmitJobOperator(
                task_id="variant_to_vcf",
                project_id=GCP_PROJECT_GENETICS,
                region=GCP_REGION,
                job_name=f"variant-to-vcf-job-{time.strftime('%Y%m%d-%H%M%S')}",
                job=batch_job,
                deferrable=False,
            )

            # ===== Partition vcf files into chunks step ======
            task_config = find_node_in_config(
                config=variant_annotation_task_group_config["nodes"],
                node_id="list_nonannotated_vcfs",
            )
            merged_vcfs = ConvertVariantsToVcfOperator(
                task_id="list_nonannotated_vcfs",
                tsv_files_glob=task_config["params"]["input_vcf_glob"],
                output_path=task_config["params"]["output_path"],
                chunk_size=task_config["params"]["chunk_size"],
            )
            # ===== Perform VEP annotations on chunks step =====
            task_config = find_node_in_config(
                variant_annotation_task_group_config["nodes"],
                node_id="vep_annotation",
            )
            task_config_params = task_config["params"]
            vep_annotation = VepAnnotateOperator(
                task_id=task_config["id"],
                vcf_input_path=task_config_params["vcf_input_path"],
                vep_output_path=task_config_params["vep_output_path"],
                vep_cache_path=task_config_params["vep_cache_path"],
                google_batch=task_config["google_batch"],
            )

            chain_dependencies(
                nodes=variant_annotation_task_group_config["nodes"],
                tasks_or_task_groups={
                    "variant_to_vcf": variant_to_vcf,
                    "list_nonannotated_vcfs": merged_vcfs,
                    "vep_annotation": vep_annotation,
                },
            )
            node_map["variant_annotation"] = variant_annotation
        # ===== END Variant annotation task group ======

        tasks = [node for node in nodes if node.get("kind", "Task") == "Task"]
        # Build individual tasks and register them as nodes.
        for task in tasks:
            dataproc_specs = config["dataproc"]
            this_task = submit_gentropy_step(
                cluster_name=dataproc_specs["cluster_name"],
                step_name=task["id"],
                python_main_module=dataproc_specs["python_main_module"],
                params=task["params"],
            )
            node_map[task["id"]] = this_task  # type: ignore

        # chain prerequisites
        chain_dependencies(nodes=config["nodes"], tasks_or_task_groups=node_map)
        generate_dataproc_task_chain(
            tasks=list(node_map.values()),  # type: ignore
            **config["dataproc"],
        )

    # DAG description:
    chain(
        # Run data transfer:
        data_transfer,
        # Once datasets are transferred, run the rest of the steps:
        genetics_etl,
    )
