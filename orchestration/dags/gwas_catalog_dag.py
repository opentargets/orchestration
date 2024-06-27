from __future__ import annotations
from airflow.decorators import dag, task, task_group
from datetime import datetime
from typing import TYPE_CHECKING
from airflow.utils.helpers import chain
from orchestration import QRCP
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from returns.result import Success, Failure, safe, Result
import logging
from urllib.parse import urljoin, urlparse
from typing import TypedDict

if TYPE_CHECKING:
    from typing import Any
    from airflow.models.xcom_arg import XComArg
    from orchestration import Dag_Params
    Config_Error = str


logger = logging.getLogger(__name__)
RUN_DATE = datetime.today()
config_file_path = "/config/config.yaml"
gwas_catalog_config_dag_id = "gwas_catalog"


# type definitions
SFTP_Transfer_Object = TypedDict("SFTP_Transfer_Object", {'source_path': str, 'destination_path': str, "destination_bucket": str})

def get_gwas_catalog_dag_config() -> Result[Dag_Params, Config_Error]:
    """Process initial base config from path to QRCP."""
    dag_params = get_current_context().get("params")
    if dag_params is None:
        logging.error("Missing DAG parameters")
        return Failure("Did not provide params")
    config_file_path = dag_params.get("config_file_path")
    if config_file_path is None:
        return Failure("Did not provide config_file_path")
    if not isinstance(config_file_path, str):
        return Failure("config_file_path parameter should be string")
    logger.info("Running Gentropy pipeline with %s", config_file_path)
    # match to the gwas catalog config DAG
    return QRCP.from_file(config_file_path).get_dag_params(gwas_catalog_config_dag_id)

def create_sftp_to_gcs_transfer_object(
    *,
    input_file: str, 
    output_file: str,
    gcs_directory: str, 
    ftp_directory: str,
) -> SFTP_Transfer_Object:
    """Method to generate transfer object that can be consumed with SFTPToGCSOperator."""
    destination_prefix = gcs_directory.replace("gs://gwas-catalog-data", "")
    transfer_object: SFTP_Transfer_Object = {
        "source_path": urljoin(ftp_directory, input_file),
        "destination_bucket": "gs://gwas-catalog-data",
        "destination_path": f"{destination_prefix}/{output_file}"
    }
    logger.info("transfer_object: %s", transfer_object)
    return transfer_object


@dag(start_date=RUN_DATE, dag_id=gwas_catalog_config_dag_id, params = {"config_file_path": config_file_path})
def gwas_catalog_dag() -> None:
    # dag_params = get_current_context()["params"]["config_file_path"]
    # first match the result of parsing config with QRCP

    @task(task_id = "read_gwas_catalog_params", multiple_outputs=True)
    def read_gwas_catalog_params() -> Dag_Params:
        match get_gwas_catalog_dag_config():
            case Success(cfg):
                return cfg
            case Failure(msg):
                raise ValueError(msg)
            case _:
                raise ValueError("Unexpected")


    # [START PREPARE CURATION MANIFEST]
    @task_group(group_id="curation")
    def prepare_manifest(gwas_catalog_params: dict[str, Any]) -> None:
        """Prepare manifests for the curration update of GWAS Catalog"""
        curation_config = gwas_catalog_params["curation"]

        @task(task_id="prepare_gwas_catalog_manifest_paths")
        def prepare_gwas_catalog_manifest_paths(curation_config : dict[str, Any] = curation_config) -> list[SFTP_Transfer_Object]:
            transfer_objects = []
            for in_file, out_file in zip(curation_config["gwas_catalog_manifest_files_ftp"], curation_config["gwas_catalog_manifest_files_gcs"]):
                transfer_object = create_sftp_to_gcs_transfer_object(
                    input_file = in_file,
                    output_file = out_file,
                    gcs_directory= curation_config["gwas_catalog_manifests_endpoint_gcs"],
                    ftp_directory=curation_config["gwas_catalog_release_ftp"]
                )
                transfer_objects.append(transfer_object)
            return  transfer_objects

        @task_group(group_id="sync_gwas_catalog_manifests")
        def sync_gwas_catalog_manifests(manifest_transfer_objects: XComArg) -> None:
            """Move all required manifests from GWAS Catalog FTP server to the GCS.
            https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping"html
            """
            
            SFTPToGCSOperator.partial(task_id="transfer_manifest_objects", sftp_conn_id = "ebi_ftp").expand_kwargs(manifest_transfer_objects)

        # @task(task_id="sync_curation_manifest")
        # def consume_manifest_file(gentropy_params: dict[str, str]) -> pl.DataFrame:
        #     """Sync curation manifest from path given by config to the bucket given by config

        #     Args:
        #         config (dict[str, str]): gentropy configuration object
        #     """
        #     manifest_url = gentropy_params["manual_curation_manifest_gh"]
        #     logging.info("Reading initial manifest from %s", manifest_url)
        #     manifest = pl.read_csv(manifest_url, has_header=True, separator="\t")
        #     return manifest

        # types are ignored, as the XArgs - result from airflow task(s) are inferred at runtime -
        # check typing in https://github.com/apache/airflow/blob/main/airflow/example_dags/tutorial_taskflow_api.py
        manifest_transfer_objects = prepare_gwas_catalog_manifest_paths(curation_config)
        process_manifests = sync_gwas_catalog_manifests(manifest_transfer_objects)
        chain(manifest_transfer_objects, process_manifests)
    # [END PREPARE MANIFESTS]

    # @task_group(group_id="gwas_catalog_harmonisation")
    # def gwas_catalog_harmonisation(gwas_catalog_params: dict[str, Any]) -> None:

    #     harmonisation_params = gwas_catalog_params["harmonisation"]
    #     summary_statistics_bucket = harmonisation_params["summary_statistics_bucket"]
    #     raw_prefix = harmonisation_params["raw_summary_statistics_prefix"]
    #     harmonised_prefix = harmonisation_params["harmonised_summary_statistics"]

    #     raw_summary_statistics = GCSListObjectsOperator.partial(
    #         task_id="list_raw_harmonised",
    #         match_glob="**/*.h.tsv.gz",
    #     ).expand(prefix=raw_prefix, bucket=summary_statistics_bucket)
    #     # List parquet files that have been previously processed
    #     harmonised_summary_statistics = GCSListObjectsOperator.partial(
    #         task_id="list_harmonised_parquet",
    #         match_glob="**/_SUCCESS",
    #     ).expand(prefix=harmonised_prefix, bucket=summary_statistics_bucket)

    #     @task(task_id = "Sanity_check_curation")
    #     def calculate_harmonisation_todo_list(
    #         raw_list: list[str], 
    #         harmonised_list: list[str],
    #         harmonised_prefix: str,
    #         raw_prefix: str
    #     ) -> list[str]:
    #         import re
    #         logging.info("Number of raw summary statistics: %s", len(raw_list))
    #         logging.info("Number of already harmonised summary statistics: %s", len(harmonised_list))
    #         todo_list = []
    #         for path in raw_list:
    #             match_result = re.search(
    #                 rf"{raw_prefix}/(.*)/(GCST\d+)/harmonised/(.*)\.h\.tsv\.gz",
    #                 path,
    #             )
    #             if match_result:
    #                 study_id = match_result.group(2)
    #                 if (
    #                     f"{harmonised_prefix}/{study_id}.parquet/_SUCCESS"
    #                     not in harmonised_list
    #                 ):
    #                     todo_list.append(path)
    #         logging.info("Number of jobs to submit: ", len(todo_list))
    #         if len(todo_list) != len(raw_list) - len(harmonised_list):
    #             raise ValueError("Incorrect number of studies inferred to harmonise")
    #         return todo_list
                  
    #     calculate_harmonisation_todo_list(
    #         raw_summary_statistics, # type: ignore
    #         harmonised_summary_statistics, # type: ignore
    #         harmonised_prefix,
    #         raw_prefix
    #     )
    gwas_catalog_params = read_gwas_catalog_params()
    chain(
        gwas_catalog_params,
        prepare_manifest(gwas_catalog_params), # type: ignore
        # gwas_catalog_harmonisation(gwas_catalog_params) # type: ignore
    )


gwas_catalog_dag()
