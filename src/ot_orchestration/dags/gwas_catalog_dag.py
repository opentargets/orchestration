"""Gwas catalog DAG."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.utils.helpers import chain
from returns.result import Failure, Success

from ot_orchestration import GWAS_CATALOG_CONFIG_DAG_ID
from ot_orchestration.task_groups.curation import (
    get_gwas_catalog_dag_config,
    create_sftp_to_gcs_transfer_object,
)
from ot_orchestration.types import FTP_Transfer_Object

if TYPE_CHECKING:
    from typing import Any
    from airflow.models.xcom_arg import XComArg
    from ot_orchestration import Dag_Params
    import pandas as pd

logging.basicConfig(level=logging.INFO)
RUN_DATE = datetime.today()


@dag(start_date=RUN_DATE, dag_id=GWAS_CATALOG_CONFIG_DAG_ID)
def gwas_catalog_dag(**kwargs: Dag_Params) -> None:
    """GWAS catalog DAG."""

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
        ) -> list[FTP_Transfer_Object]:
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
        manifest_transfer_objects = prepare_gwas_catalog_manifest_paths(curation_config)
        sync_gwas_catalog_manifests(manifest_transfer_objects)

    @task(task_id="gwas_catalog_manifest")
    def get_manifest(gwas_catalog_params: dict[str, Any]) -> pd.DataFrame:
        """Get original manifest for gwas catalog."""
        manifest_path = gwas_catalog_params["curation"]["manual_curation_manifest_gh"]
        import pandas as pd

        return pd.read_csv(manifest_path, sep="\t")

    gwas_catalog_params = read_gwas_catalog_params()
    chain(
        gwas_catalog_params,
        gwas_catalog_curation(gwas_catalog_params),  # type: ignore
        get_manifest(gwas_catalog_params),  # type: ignore
    )


gwas_catalog_dag()
