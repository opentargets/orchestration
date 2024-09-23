"""Airflow DAG for the preprocessing of GWAS Catalog's harmonised summary statistics and curated associations."""

from __future__ import annotations

from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.task_group import TaskGroup

from ot_orchestration.utils import (
    chain_dependencies,
    find_node_in_config,
    read_yaml_config,
)
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from ot_orchestration.utils.dataproc import (
    create_cluster,
    delete_cluster,
    submit_gentropy_step,
)
from ot_orchestration.utils.path import GCSPath

CONFIG_PATH = Path(__file__).parent / "config" / "gwas_catalog_preprocess.yaml"
config = read_yaml_config(CONFIG_PATH)
sumstat_glob = GCSPath(config["gwas_catalog_harmonised_sumstat_glob"])
harmonised_sumstat_list = GCSPath(config["harmonised_sumstat_list"])
sumstat_config = find_node_in_config(config["nodes"], "summary_statistics_processing")
top_hits_config = find_node_in_config(config["nodes"], "top_hits_processing")


def upload_harmonized_study_list(
    concatenated_studies: str, bucket_name: str, object_name: str
) -> None:
    """This function uploads file to GCP.

    Args:
        concatenated_studies (str): Concatenated list of harmonized summary statistics.
        bucket_name (str): Bucket name
        object_name (str): Name of the object
    """
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=concatenated_studies,
        encoding="utf-8",
    )


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics — GWAS Catalog preprocess",
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    # Getting list of folders (each a gwas study with summary statistics)
    list_harmonised_sumstats = GCSListObjectsOperator(
        task_id="list_harmonised_sumstats",
        bucket=sumstat_glob.bucket,
        prefix=sumstat_glob.segments["prefix"],
        match_glob=sumstat_glob.segments["filename"],
    )

    # Upload resuling list to a bucket:
    upload_task = PythonOperator(
        task_id="uploader",
        python_callable=upload_harmonized_study_list,
        op_kwargs={
            "concatenated_studies": '{{ "\n".join(ti.xcom_pull( key="return_value", task_ids="list_harmonised_sumstats")) }}',
            "bucket_name": harmonised_sumstat_list.bucket,
            "object_name": harmonised_sumstat_list.path,
        },
    )

    # Processing curated GWAS Catalog top-bottom:
    with TaskGroup(group_id=top_hits_config["id"]) as top_hits_processing:
        tasks = {}
        if top_hits_config["nodes"]:
            for step in top_hits_config["nodes"]:
                task = submit_gentropy_step(
                    cluster_name=config["dataproc"]["cluster_name"],
                    step_name=step["id"],
                    python_main_module=config["dataproc"]["python_main_module"],
                    params=step["params"],
                )
                tasks[step["id"]] = task
            chain_dependencies(
                nodes=top_hits_config["nodes"], tasks_or_task_groups=tasks
            )  # type: ignore

    # Processing summary statistics from GWAS Catalog:
    with TaskGroup(group_id=sumstat_config["id"]) as summary_statistics_processing:
        tasks = {}
        if sumstat_config["nodes"]:
            for step in sumstat_config["nodes"]:
                task = submit_gentropy_step(
                    cluster_name=config["dataproc"]["cluster_name"],
                    step_name=step["id"],
                    python_main_module=config["dataproc"]["python_main_module"],
                    params=step["params"],
                )
                tasks[step["id"]] = task
            chain_dependencies(
                nodes=sumstat_config["nodes"], tasks_or_task_groups=tasks
            )  # type: ignore

    # DAG description:
    chain(
        create_cluster(
            cluster_name=config["dataproc"]["cluster_name"],
            autoscaling_policy=config["dataproc"]["autoscaling_policy"],
            num_workers=config["dataproc"]["num_workers"],
            cluster_metadata=config["dataproc"]["cluster_metadata"],
            cluster_init_script=config["dataproc"]["cluster_init_script"],
        ),
        list_harmonised_sumstats,
        upload_task,
        top_hits_processing,
        summary_statistics_processing,
        delete_cluster(config["dataproc"]["cluster_name"]),
    )

if __name__ == "__main__":
    pass
