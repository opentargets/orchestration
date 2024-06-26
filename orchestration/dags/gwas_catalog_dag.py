from __future__ import annotations
from airflow.decorators import dag, task, task_group
from datetime import datetime
from pathlib import Path
import sys
from typing import TYPE_CHECKING
from airflow.utils.helpers import chain
from airflow.operators.python import get_current_context
from orchestration import QRCP
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from returns.result import Success, Failure
import logging
import polars as pl
from urllib.parse import urljoin


if TYPE_CHECKING:
    from typing import Any
    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
    from airflow.models.xcom_arg import XComArg

logger = logging.getLogger(__name__)
RUN_DATE = datetime.today()
config_file_path = "/config/config.yaml"
gwas_catalog_config_dag_id = "gwas_catalog"

def get_task_instance() -> TaskInstance | TaskInstancePydantic:
    context = get_current_context()
    task_instance = context.get("ti")
    if task_instance is None:
        raise RuntimeError("Could not find the task instance")
    return task_instance

def pull_dag_params(*, read_config_task_id: str = "process_config.read_config", dag: str = "gwas_catalog") -> dict[str, Any]:
    """Generic Method to pull the config from the parser task

    Returns:
        dict[str, str]: config
    """
    logger.info("Pulling config file from read_config task")
    task_instance = get_task_instance()
    config = task_instance.xcom_pull(read_config_task_id)
    config = QRCP.deserialize(config)
    gwas_catalog_params = config.get_dag_params("gwas_catalog")

    logger.info("Extracting gwas_catalog_dag paramters from config")
    match gwas_catalog_params:
        case Success(params):
            logging.info("GWAS Catalog dag params: %s", params)
            return params
        case Failure(msg):
            raise ValueError(msg)
        case _:
            raise RuntimeError("Unexpected bahavior during pulling %s dag parameters", dag)


   
@dag(start_date=RUN_DATE, dag_id = gwas_catalog_config_dag_id)
def gwas_catalog_dag(config_file_path: str = config_file_path) -> None:

    # [START CONFIG PROCESSING]
    @task_group(group_id="process_config")
    def process_config(config_file_path: str = config_file_path) -> None:

        @task.short_circuit()
        def check_if_config_exists(config_file_path: str) -> bool:
            logger.info("Running Gentropy pipeline with %s", config_file_path)
            config_exists = Path(config_file_path).exists()
            logger.info("Checking if config file exists...")
            if not config_exists:
                logging.error(
                    f"Config at {str(config_file_path)} was not found")
                sys.exit(1)
            logger.info("Config file exists")
            return config_exists

        @task.python(task_id="read_config")
        def parse_config_file(config_file_path: str) -> dict[str, Any]:
            # perform validation by the QRCP model once.
            logger.info("Validating configuration file")
            return QRCP.from_file(config_file_path).serialize()

        config_exists = check_if_config_exists(config_file_path)
        config = parse_config_file(config_file_path)

        chain(
            config_exists,
            config
        )
    # [END CONFIG PROCESSING]

    # [START PREPARE CURATION MANIFEST]
    @task_group(group_id="prepare_manifest")
    def prepare_manifest(gentropy_params: dict[str, Any]) -> None:
        """ "Prepare manifests for the curration update of GWAS Catalog"""

        @task(task_id="prepare_gwas_catalog_manifest_paths")
        def prepare_gwas_catalog_manifest_paths(
            gentropy_params: dict[str, Any]
        ) -> list[dict[str, Any]]:
            transfer_objects = []
            for input_file, output_file in zip(
                gentropy_params["gwas_catalog_manifest_files_ftp"],
                gentropy_params["gwas_catalog_manifest_files_gcs"],
            ):
                transfer_object = {
                    "source_path": urljoin(
                        gentropy_params["gwas_catalog_release_ftp"],
                        input_file,
                    ),
                    "destination_bucket": gentropy_params["gwas_catalog_data_bucket"],
                    "destination_path": output_file,
                }
                logger.info("transfer_object: %s", transfer_object)
                transfer_objects.append(transfer_object)
            return transfer_objects

        @task_group(group_id="sync_gwas_catalog_manifests")
        def sync_gwas_catalog_manifests(
            manifest_transfer_objects: XComArg,
        ) -> None:
            """Move all required manifests from GWAS Catalog FTP server to the GCS.
            https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html

            Args:
                manifest_transfer_objects (list[dict[str, str]]): list of parameters to SFTPToGCSOperator
            """
            SFTPToGCSOperator.partial(task_id = "transfer_manifest_objects").expand_kwargs(manifest_transfer_objects)

        @task(task_id="sync_curation_manifest")
        def consume_manifest_file(gentropy_params: dict[str, str]) -> pl.DataFrame:
            """Sync curation manifest from path given by config to the bucket given by config

            Args:
                config (dict[str, str]): gentropy configuration object
            """
            manifest_url = gentropy_params["manual_curation_manifest_gh"]
            logging.info("Reading initial manifest from %s", manifest_url)
            manifest = pl.read_csv(
                manifest_url, has_header=True, separator="\t")
            return manifest
        
        # types are ignored, as the XArgs - result from airflow task(s) are inferred at runtime -
        # check typing in https://github.com/apache/airflow/blob/main/airflow/example_dags/tutorial_taskflow_api.py
        manifest_transfer_objects = prepare_gwas_catalog_manifest_paths(gentropy_params) # type: ignore
        process_manifests = [
            sync_gwas_catalog_manifests(manifest_transfer_objects),
            consume_manifest_file(gentropy_params) # type: ignore
        ]
        chain(
            manifest_transfer_objects,
            process_manifests
        )
    # [END PREPARE CURATION MANIFEST]

    # [START PULL GWAS CATALOG CURATION MANIFEST]
    @task(task_id="pull_gwas_catalog_manifest")
    def pull_gwas_catalog_manifest(task_id: str) -> pl.DataFrame:
        """Pull manifest to the next tasks"""
        ti = get_task_instance()
        logger.info("Pulling GWAS Catalog curation manifest")
        manifest = ti.xcom_pull(task_id)
        print(manifest)
        return manifest
    # [END PULL GWAS CATALOG CURATION MANIFEST]

    @task_group(group_id = "gwas_catalog_harmonisation")
    def gwas_catalog_harmonisation(gwas_catalog_params: dict[str, Any]) -> None:
        # pull params
        
        harmonisation_params = gwas_catalog_params["harmonisation"]
        summary_statistics_bucket = harmonisation_params["summary_statistics_bucket"]
        raw_prefix = harmonisation_params["raw_summary_statistics_prefix"]
        harmonised_prefix = harmonisation_params["harmonised_summary_statistics"]

        list_inputs = GCSListObjectsOperator.partial(
            task_id="list_raw_harmonised",
            match_glob="**/*.h.tsv.gz",
        ).expand(prefix=raw_prefix, bucket=summary_statistics_bucket)
        # List parquet files that have been previously processed
        list_outputs = GCSListObjectsOperator.partial(
            task_id="list_harmonised_parquet",
            match_glob="**/_SUCCESS",
        ).expand(prefix=harmonised_prefix, bucket=summary_statistics_bucket)
 

    config_processing = process_config(config_file_path)
    gentropy_params = pull_dag_params(dag=gwas_catalog_config_dag_id)
    manifest = prepare_manifest(gentropy_params)
    print_manifest = pull_gwas_catalog_manifest("prepare_manifest.consume_manifest_file")
    
    
    chain(
        config_processing,
        gentropy_params,
        manifest,
        print_manifest,
        gwas_catalog_harmonisation(gentropy_params)
    )


  
gwas_catalog_dag()
