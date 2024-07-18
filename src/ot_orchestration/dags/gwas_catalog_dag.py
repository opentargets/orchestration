"""Gwas catalog DAG."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.utils.helpers import chain

from ot_orchestration import GWAS_CATALOG_CONFIG_DAG_ID
from ot_orchestration.task_groups.curation import (
    get_config,
    create_sftp_to_gcs_transfer_object,
)
from ot_orchestration.types import FTP_Transfer_Object
import pandas as pd

if TYPE_CHECKING:
    from ot_orchestration import Dag_Params
    import pandas as pd

logging.basicConfig(level=logging.INFO)
RUN_DATE = datetime.today()


@dag(start_date=RUN_DATE, dag_id=GWAS_CATALOG_CONFIG_DAG_ID)
def gwas_catalog_dag(**kwargs: Dag_Params) -> None:
    """GWAS catalog DAG."""
    # [START PREPARE CURATION MANIFEST]
    @task_group(group_id="curation")
    def gwas_catalog_curation() -> None:
        """Prepare manifests for the curration update of GWAS Catalog."""

        @task(task_id="prepare_gwas_catalog_manifest_paths")
        def prepare_gwas_catalog_manifest_paths() -> list[FTP_Transfer_Object]:
            """Prepare transfer objects for the gwas catalog manifests from ftp to gcs."""
            curation_config = get_config()["DAGS"][0]["params"]["curation"]
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

        manifest_transfer_objects = prepare_gwas_catalog_manifest_paths()
        SFTPToGCSOperator.partial(
            task_id="transfer_manifest_objects", sftp_conn_id="ebi_ftp"
        ).expand_kwargs(manifest_transfer_objects)

    @task(task_id="gwas_catalog_manifest", trigger_rule="all_done")
    def get_manifest() -> pd.DataFrame:
        """Get original manifest for gwas catalog."""
        manifest_path = get_config()["DAGS"][0]["params"]["curation"][
            "manual_curation_manifest_gh"
        ]

        return pd.read_csv(manifest_path, sep="\t")

    chain(
        gwas_catalog_curation(),
        get_manifest(),
    )


gwas_catalog_dag()
