"""Test DAG to prototype data transfer."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup

from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import generate_dataproc_task_chain
from ot_orchestration.utils.path import GCSPath
from ot_orchestration.utils.utils import check_gcp_folder_exists, read_yaml_config

SOURCE_CONFIG_FILE_PATH = Path(__file__).parent / "config" / "genetics_etl.yaml"
config = read_yaml_config(SOURCE_CONFIG_FILE_PATH)
steps = config["steps"]
gwas_catalog_manifests_path = GCSPath(config["gwas_catalog_manifests_path"])
l2g_gold_standard_path = GCSPath(config["l2g_gold_standard_path"])
release_dir = GCSPath(config["release_dir"])

# Files to move:
DATA_TO_MOVE = {
    # GWAS Catalog manifest files:
    "gwas_catalog_manifests": {
        "source_bucket": gwas_catalog_manifests_path.bucket,
        "source_object": gwas_catalog_manifests_path.path,
        "destination_bucket": release_dir.bucket,
        "destination_object": f"{release_dir.path}/manifests/",
    },
    # L2G gold standard:
    "l2g_gold_standard": {
        "source_bucket": l2g_gold_standard_path.bucket,
        "source_object": l2g_gold_standard_path.path,
        "destination_bucket": release_dir.bucket,
        "destination_object": f"{release_dir.path}/locus_to_gene_gold_standard.json",
    },
}


# This operator meant to fail the DAG if the release folder exists:
ensure_release_folder_not_exists = ShortCircuitOperator(
    task_id="test_release_folder_exists",
    python_callable=lambda bucket, path: not check_gcp_folder_exists(bucket, path),
    op_kwargs={"bucket": release_dir.bucket, "path": release_dir.path},
)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics ETL workflow",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
):
    # Compiling tasks for moving data to the right place:
    with TaskGroup(group_id="data_transfer") as data_transfer:
        # Defining the tasks to execute in the task group:
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
        # Parse and define all steps and their prerequisites.
        tasks = {}
        for step in steps:
            step_id = step["id"]
            step_params = convert_params_to_hydra_positional_arg(step)
            this_task = submit_step(
                cluster_name=config["cluster_name"],
                step_id=step_id,
                task_id=step_id,
                other_args=step_params,
            )
            # Chain prerequisites.
            tasks[step_id] = this_task
            for prerequisite in step.get("prerequisites", []):
                this_task.set_upstream(tasks[prerequisite])

        generate_dag(cluster_name=config["cluster_name"], tasks=list(tasks.values()))

    # DAG description:
    chain(
        # Test that the release folder doesn't exist:
        ensure_release_folder_not_exists,
        # Run data transfer:
        data_transfer,
        # Once datasets are transferred, run the rest of the steps:
        genetics_etl,
    )
