"""Gwas catalog DAG."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, TypedDict
from urllib.parse import urljoin

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.transfers.sftp_to_gcs import \
    SFTPToGCSOperator
from airflow.utils.helpers import chain
from returns.result import Failure, Result, Success

from ot_orchestration import QRCP

if TYPE_CHECKING:
    from typing import Any

    from airflow.models.xcom_arg import XComArg

    from ot_orchestration import Dag_Params
    import polars as pl

    Config_Error = str


logger = logging.getLogger(__name__)
RUN_DATE = datetime.today()
config_file_path = "/config/config.yaml"
gwas_catalog_config_dag_id = "GWAS_Catalog"


# type definitions
SFTP_Transfer_Object = TypedDict(
    "SFTP_Transfer_Object",
    {"source_path": str, "destination_path": str, "destination_bucket": str},
)


def get_gwas_catalog_dag_config() -> Result[Dag_Params, Config_Error]:
    """Process initial base config from path to QRCP."""
    dag_run_params = get_current_context().get("params")
    if dag_run_params is None or not dag_run_params:
        return Failure(
            "No params provided to the DAG run, ensure that you are triggering gwas_catalog_dag with config.json content"
        )
    # the kwargs comes from the @dag function parameters
    airflow_config = dag_run_params.get("kwargs")
    if airflow_config is None or not airflow_config:
        return Failure(
            "Empty or none configuration provided, ensure that you are triggering gwas_catalog_dag with config.json content"
        )
    # match to the gwas catalog config DAG from the full config
    return QRCP(conf=airflow_config).get_dag_params(gwas_catalog_config_dag_id)


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
        "destination_path": f"{destination_prefix}/{output_file}",
    }
    logger.info("transfer_object: %s", transfer_object)
    return transfer_object


@dag(
    start_date=RUN_DATE,
    dag_id=gwas_catalog_config_dag_id,
)
def gwas_catalog_dag(**kwargs: Dag_Params) -> None:
    """GWAS catalog DAG."""
    # dag_params = get_current_context()["params"]["config_file_path"]
    # first match the result of parsing config with QRCP

    @task(task_id="read_gwas_catalog_params", multiple_outputs=True)
    def read_gwas_catalog_params() -> Dag_Params:
        """Read gwas catalog prams from pipeline config."""
        match get_gwas_catalog_dag_config():
            case Success(cfg):
                return cfg
            case Failure(msg):
                raise AirflowException(msg)
            case _:
                raise AirflowException("Unexpected execution path")

    # [START PREPARE CURATION MANIFEST]
    @task_group(group_id="curation")
    def gwas_catalog_curation(gwas_catalog_params: dict[str, Any]) -> None:
        """Prepare manifests for the curration update of GWAS Catalog."""
        curation_config = gwas_catalog_params["curation"]

        @task(task_id="prepare_gwas_catalog_manifest_paths")
        def prepare_gwas_catalog_manifest_paths(
            curation_config: dict[str, Any] = curation_config,
        ) -> list[SFTP_Transfer_Object]:
            """Prepare transfer objects for the gwas catalog manifests from ftp to gcs."""
            transfer_objects = []
            for in_file, out_file in zip(
                curation_config["gwas_catalog_manifest_files_ftp"],
                curation_config["gwas_catalog_manifest_files_gcs"],
            ):
                transfer_object = create_sftp_to_gcs_transfer_object(
                    input_file=in_file,
                    output_file=out_file,
                    gcs_directory=curation_config[
                        "gwas_catalog_manifests_endpoint_gcs"
                    ],
                    ftp_directory=curation_config["gwas_catalog_release_ftp"],
                )
                transfer_objects.append(transfer_object)
            return transfer_objects

        @task_group(group_id="sync_gwas_catalog_manifests")
        def sync_gwas_catalog_manifests(manifest_transfer_objects: XComArg) -> None:
            """Move all required manifests from GWAS Catalog FTP server to the GCS.

            https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping
            """
            SFTPToGCSOperator.partial(
                task_id="transfer_manifest_objects", sftp_conn_id="ebi_ftp"
            ).expand_kwargs(manifest_transfer_objects)

        # types are ignored, as the XArgs - result from airflow task(s) are inferred at runtime -
        # check typing in https://github.com/apache/airflow/blob/main/airflow/example_dags/tutorial_taskflow_api.py
        prepare_gwas_catalog_manifest_paths(curation_config)
        # sync_gwas_catalog_manifests(manifest_transfer_objects)

    @task(task_id = "gwas_catalog_manifest")
    def get_manifest(gwas_catalog_params: dict[str, Any]) -> pl.DataFrame:
        """Get original manifest for gwas catalog."""
        manifest_path = gwas_catalog_params["curation"]["manual_curation_manifest_gh"]
        import polars as pl
        return pl.read_csv(manifest_path, separator="\t")

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
        gwas_catalog_curation(gwas_catalog_params),  # type: ignore
        get_manifest(gwas_catalog_params) # type: ignore
        # gwas_catalog_harmonisation(gwas_catalog_params) # type: ignore
    )


gwas_catalog_dag()
