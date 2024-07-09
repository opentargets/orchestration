"""Curation tasks and task groups."""

from returns.result import Result, Failure
from airflow.operators.python import get_current_context
from ot_orchestration import Dag_Params, QRCP, GWAS_CATALOG_CONFIG_DAG_ID
import logging
from urllib.parse import urljoin
from ot_orchestration.types import FTP_Transfer_Object


def get_gwas_catalog_dag_config() -> Result[Dag_Params, str]:
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
    return QRCP(conf=airflow_config).get_dag_params(GWAS_CATALOG_CONFIG_DAG_ID)


def create_sftp_to_gcs_transfer_object(
    *,
    input_file: str,
    output_file: str,
    gcs_directory: str,
    ftp_directory: str,
) -> FTP_Transfer_Object:
    """Method to generate transfer object that can be consumed with FTPToGCSOperator."""
    destination_prefix = gcs_directory.replace("gs://gwas-catalog-data", "")
    transfer_object: FTP_Transfer_Object = {
        "source_path": urljoin(ftp_directory, input_file),
        "destination_bucket": "gs://gwas-catalog-data",
        "destination_path": f"{destination_prefix}/{output_file}",
    }
    logging.info("transfer_object: %s", transfer_object)
    return transfer_object
