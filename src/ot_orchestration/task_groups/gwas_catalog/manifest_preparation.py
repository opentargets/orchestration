"""Manifest preparation task group."""

from airflow.decorators import task, task_group
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from ot_orchestration.types import Manifest_Object
from ot_orchestration.utils import get_gwas_catalog_dag_params, GCSIOManager, GCSPath
from airflow.utils.helpers import chain
import pandas as pd

FILTER_FILE = "/opt/airflow/config/filter.csv"


@task_group(group_id="manifest_preparation")
def gwas_catalog_manifest_preparation():
    """Prepare initial manifest."""
    existing_manifest_paths = GCSListObjectsOperator(
        task_id="list_existing_manifests",
        bucket="{{ params.DAGS.GWAS_Catalog.staging_bucket }}",
        prefix="{{ params.DAGS.GWAS_Catalog.staging_prefix }}",
        match_glob="**/manifest.json",
    )

    @task(task_id="read_exisitng_manifests")
    def read_existing_manifests(manifest_paths: list[GCSPath]) -> list[Manifest_Object]:
        """Read manifests that already exist in staging bucket."""
        # recreate the paths with the gs://{bucket_name}/
        params = get_gwas_catalog_dag_params()
        staging_bucket: str = params["staging_bucket"]
        manifest_paths = [
            f"gs://{staging_bucket}/{manifest_path}" for manifest_path in manifest_paths
        ]
        return GCSIOManager().load_many(manifest_paths)

    existing_manifests = read_existing_manifests(existing_manifest_paths.output)  # type: ignore

    raw_sumstats_paths = GCSListObjectsOperator(
        task_id="list_raw_harmonised",
        bucket="{{ params.DAGS.GWAS_Catalog.raw_sumstats_bucket }}",
        prefix="{{ params.DAGS.GWAS_Catalog.raw_sumstats_prefix }}",
        match_glob="**/*.h.tsv.gz",
    )

    # @task(task_id="run-on-test-batch")

    @task(task_id="generate_new_manifests")
    def generate_new_manifests(
        raw_sumstats_paths: list[str], existing_manifests: list[Manifest_Object]
    ) -> list[Manifest_Object]:
        """Task to generate manifest files for the new studies.

        The steps to run here are:
        (1) extract studies that do not have the structure in the
            staging bucket and do not have the manifest.json file inside -
        we expect that these studies were just synchronised from
        GWAS_Catalog to Open Targets google storage raw_sumstats bucket.
        (2) for the studies in (1) generate manifest template and
        save it to create a staging directory for the study outputs
        (3) extend (2) with existing manifests for the next steps
        """
        params = get_gwas_catalog_dag_params()
        staging_bucket: str = params["staging_bucket"]
        staging_prefix: str = params["staging_prefix"]
        harmonised_prefix: str = params["harmonised_prefix"]
        raw_sumstat_bucket: str = params["raw_sumstats_bucket"]
        qc_prefix: str = params["qc_prefix"]
        staging_dir = f"{staging_bucket}/{staging_prefix}"
        studies_in_staging_bucket = [
            manifest["studyId"] for manifest in existing_manifests
        ]
        manifests = []
        for raw_path in raw_sumstats_paths:
            study_id = raw_path.split("/")[2]
            if study_id not in studies_in_staging_bucket:
                staging_path = f"{staging_dir}/{study_id}"
                partial_manifest = {
                    "studyId": study_id,
                    "rawPath": f"gs://{raw_sumstat_bucket}/{raw_path}",
                    "manifestPath": f"gs://{staging_path}/manifest.json",
                    "harmonisedPath": f"gs://{staging_path}/{harmonised_prefix}",
                    "qcPath": f"gs://{staging_path}/{qc_prefix}",
                    "passHarmonisation": None,
                    "passQC": None,
                    # fields from the curation file
                    "studyType": None,
                    "analysisFlag": None,
                    "isCurated": None,
                    "pubmedId": None,
                }
                manifests.append(partial_manifest)
                print(partial_manifest)
        return manifests

    @task(task_id="amend_curation_metadata")
    def amend_curation_metadata(
        new_manifests: list[Manifest_Object], existing_manifests: list[Manifest_Object]
    ):
        """Read curation file and add it to the partial manifests."""
        all_manifests = new_manifests + existing_manifests
        params = get_gwas_catalog_dag_params()
        curation_path: str = params["manual_curation_manifest_gh"]
        # the curation_df should have fields that match the manifest file
        # studyId, studyType, analysisFlag, isCurated, pubmedId
        curation_df = pd.read_csv(curation_path, sep="\t").drop(
            columns=["publicationTitle", "traitFromSource", "qualityControl"]
        )
        manifest_df = pd.DataFrame.from_records(all_manifests)
        # overwrite old curation columns with the data that comes from the curation_manifest
        # this means that we need to update the None values that were generated from scratch
        manifest_df = manifest_df.drop(
            columns=["studyType", "analysisFlag", "isCurated", "pubmedId"]
        )
        curated_manifest = manifest_df.merge(curation_df, how="left", on="studyId")
        if params["perform_test_on_curated_batch"]:
            filter_df = pd.read_csv(FILTER_FILE, sep=",")[["studyId"]]
            print(filter_df)
            curated_manifest = filter_df.merge(
                curated_manifest, how="left", on="studyId"
            )
        curated_manifests = curated_manifest.replace({float("nan"): None}).to_dict(
            "records"
        )
        return curated_manifests

    @task(task_id="save_manifests")
    def save_manifests(manifests: list[Manifest_Object]) -> None:
        """Write manifests to persistant storage."""
        manifest_paths = [manifest["manifestPath"] for manifest in manifests]
        GCSIOManager().dump_many(manifests, manifest_paths)

    new_manifests = generate_new_manifests(
        raw_sumstats_paths.output, existing_manifests
    )  # type: ignore
    manifests_with_curation = amend_curation_metadata(new_manifests, existing_manifests)  # type: ignore
    chain(
        existing_manifests,
        new_manifests,
        manifests_with_curation,
        save_manifests(manifests_with_curation),
    )


__all__ = ["gwas_catalog_manifest_preparation"]
