"""Curation tasks and task groups."""

import logging
from urllib.parse import urljoin
from ot_orchestration.types import FTP_Transfer_Object


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
