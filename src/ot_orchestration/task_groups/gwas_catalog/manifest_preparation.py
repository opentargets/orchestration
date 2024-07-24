"""Manifest preparation task group."""

from airflow.decorators import task, task_group
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from ot_orchestration.types import Manifest_Object
from ot_orchestration.utils import GCSIOManager, get_step_params, get_full_config
from airflow.utils.helpers import chain
from ot_orchestration.utils.manifest import extract_study_id_from_path
import logging
import pandas as pd
from airflow.models.taskinstance import TaskInstance


FILTER_FILE = "/opt/airflow/config/filter.csv"
TASK_GROUP_ID = "manifest_preparation"


@task_group(group_id=TASK_GROUP_ID)
def gwas_catalog_manifest_preparation():
    """Prepare initial manifest."""
    options = ["FORCE", "RESUME", "CONTINUE"]
    print(options)
    existing_manifest_paths = GCSListObjectsOperator(
        task_id="list_existing_manifests",
        bucket="{{ params.steps.manifest_preparation.staging_bucket }}",
        prefix="{{ params.steps.manifest_preparation.staging_prefix }}",
        match_glob="**/manifest.json",
    ).output

    raw_sumstats_paths = GCSListObjectsOperator(
        task_id="list_raw_harmonised",
        bucket="{{ params.steps.manifest_preparation.raw_sumstats_bucket }}",
        prefix="{{ params.steps.manifest_preparation.raw_sumstats_prefix }}",
        match_glob="**/*.h.tsv.gz",
    ).output

    @task(task_id="get_new_sumstats")
    def get_new_sumstats(
        raw_sumstats_paths: list[str], existing_manifest_paths: list[str]
    ) -> dict[str, str]:
        """Get new sumstats."""
        processed = {extract_study_id_from_path(p) for p in existing_manifest_paths}
        logging.info("ALREADY PROCESSED STUDIES: %s", len(processed))
        all = {extract_study_id_from_path(p): p for p in raw_sumstats_paths}
        logging.info("ALL STUDIES (INCLUDING NOT PROCESSED): %s", len(all))
        new = {key: val for key, val in all.items() if key not in processed}
        logging.info("NEW STUDIES UNPROCESSED: %s", len(new))
        return new

    new_sumstats = get_new_sumstats(raw_sumstats_paths, existing_manifest_paths)

    @task(task_id="generate_new_manifests")
    def generate_new_manifests(new_sumstats: dict[str, str]) -> list[Manifest_Object]:
        """Task to generate manifest files for the new studies."""
        params = get_step_params("manifest_preparation")
        # params from the configuration
        logging.info("USING FOLLOWING PARAMS: %s", params)
        raw_sumstat_bucket = params["raw_sumstats_bucket"]
        staging_bucket = params["staging_bucket"]
        staging_prefix = params["staging_prefix"]
        harmonised_prefix = params["harmonised_result_path_prefix"]
        qc_prefix = params["qc_result_path_prefix"]

        # prepare manifests for the new studies
        manifests = []
        for study_id, raw_sumstat_path in new_sumstats.items():
            staging_path = f"{staging_bucket}/{staging_prefix}/{study_id}"
            partial_manifest = {
                "studyId": study_id,
                "rawPath": f"gs://{raw_sumstat_bucket}/{raw_sumstat_path}",
                "manifestPath": f"gs://{staging_path}/manifest.json",
                "harmonisedPath": f"gs://{staging_path}/{harmonised_prefix}",
                "qcPath": f"gs://{staging_path}/{qc_prefix}",
                "passHarmonisation": False,
                "passQC": False,
                "passClumping": False,
            }
            manifests.append(partial_manifest)
            logging.info(partial_manifest)
        return manifests

    new_manifests = generate_new_manifests(new_sumstats)

    @task(task_id="amend_curation_metadata")
    def amend_curation_metadata(new_manifests: list[Manifest_Object]):
        """Read curation file and add it to the partial manifests."""
        params = get_step_params("manifest_preparation")
        logging.info("USING FOLLOWING PARAMS: %s", params)
        curation_path = params["manual_curation_manifest"]
        logging.info("CURATING MANIFESTS WITH: %s", curation_path)
        curation_df = pd.read_csv(curation_path, sep="\t").drop(
            columns=["publicationTitle", "traitFromSource", "qualityControl"]
        )
        new_manifests = (
            pd.DataFrame.from_records(new_manifests)
            .merge(curation_df, how="left", on="studyId")
            .replace({float("nan"): None})
            .to_dict("records")
        )
        for new_manifest in new_manifests:
            logging.info("NEW MANIFESTS WITH CURATION METADATA: %s", new_manifest)
        return new_manifests

    new_manifests_with_curation = amend_curation_metadata(new_manifests)

    @task(task_id="save_manifests")
    def save_manifests(manifests: list[Manifest_Object]) -> list[Manifest_Object]:
        """Write manifests to persistant storage."""
        manifest_paths = [manifest["manifestPath"] for manifest in manifests]
        logging.info("MANIFEST PATHS: %s", manifest_paths)
        GCSIOManager().dump_many(manifests, manifest_paths)
        return manifests

    saved_manifests = save_manifests(new_manifests_with_curation)

    @task(task_id="choose_manifest_paths")
    def choose_manifest_paths(manifests: list[Manifest_Object]) -> list[str]:
        """Choose manifests to pass to the next."""
        return [
            manifest["manifestPath"] for manifest in manifests if manifest["isCurated"]
        ]

    filtered_manifests = choose_manifest_paths(saved_manifests)

    @task(task_id="save_config")
    def save_config(task_instance: TaskInstance | None = None) -> str:
        """Save configuration for batch processing."""
        run_id = task_instance.run_id
        params = get_step_params("manifest_preparation")
        full_config = get_full_config().serialize()
        config_path = f"gs://{params['staging_bucket']}/{params['staging_prefix']}/{run_id}/config.yaml"
        logging.info("DUMPING CONFIG TO THE FOLLOWING PATH: %s", config_path)
        GCSIOManager().dump(gcs_path=config_path, data=full_config)
        return config_path

    saved_config_path = save_config()

    chain(
        [existing_manifest_paths, raw_sumstats_paths],
        new_manifests,
        new_manifests_with_curation,
        saved_manifests,
        filtered_manifests,
        saved_config_path,
    )


__all__ = ["gwas_catalog_manifest_preparation"]
