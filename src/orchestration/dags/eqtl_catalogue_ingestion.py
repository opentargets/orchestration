"""Airflow DAG to extract credible sets and a study index from eQTL Catalogue's finemapping results."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

from orchestration.operators.dataproc import CustomClusterConfig
from orchestration.utils import find_node_in_config, read_yaml_config
from orchestration.utils.common import GCP_PROJECT_GENETICS, shared_dag_args, shared_dag_kwargs
from orchestration.utils.dataproc import create_cluster, delete_cluster, submit_gentropy_step
from orchestration.utils.path import GCSPath

CONFIG_PATH = Path(__file__).parent / "config" / "eqtl_catalogue_ingestion.yaml"
config = read_yaml_config(CONFIG_PATH)

with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — eQTL preprocess",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    # SuSIE fine mapping results are stored as gzipped files in a GCS bucket.
    # To improve processing performance, we decompress the files before processing to a temporary location in GCS.
    decompression_job = DataflowTemplatedJobStartOperator(
        task_id="decompress_susie_outputs",
        template="gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files",
        location="europe-west1",
        project_id=GCP_PROJECT_GENETICS,
        parameters={
            "inputFilePattern": config["eqtl_catalogue_raw_susie_glob"],
            "outputDirectory": config["eqtl_catalogue_decompressed_susie_path"],
            "outputFailureFile": config["decompression_logs"],
        },
    )
    step_config = find_node_in_config(config["nodes"], "eqtl_catalogue")
    if step_config is None:
        raise ValueError("Step configuration for 'eqtl_catalogue' not found in config file.")
    ingestion_job = submit_gentropy_step(
        cluster_name=config["dataproc"]["cluster_name"],
        step_name=step_config["id"],
        params=step_config["params"],
    )
    decompressed_files_path = GCSPath(config["eqtl_catalogue_decompressed_susie_path"])

    delete_decompressed_job = GCSDeleteObjectsOperator(
        task_id="delete_decompressed_files",
        bucket_name=decompressed_files_path.bucket,
        prefix=f"{decompressed_files_path.segments['filename']}/",
    )

    chain(
        decompression_job,
        create_cluster(
            cluster_config=CustomClusterConfig(**config["dataproc"]["cluster_config"]),
            cluster_name=config["dataproc"]["cluster_name"],
        ),
        ingestion_job,
        [delete_decompressed_job, delete_cluster(config["dataproc"]["cluster_name"])],
    )


if __name__ == "__main__":
    dag.test()
