"""Manifest preparation task group."""

from airflow.decorators import task, task_group
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from ot_orchestration.types import Manifest_Object
from ot_orchestration.utils import GCSIOManager, get_step_params, get_full_config
from airflow.models.baseoperator import chain
from ot_orchestration.utils.manifest import extract_study_id_from_path
import logging
import pandas as pd
from airflow.models.taskinstance import TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

FILTER_FILE = "/opt/airflow/config/filter.csv"
TASK_GROUP_ID = "manifest_preparation"


@task_group(group_id=TASK_GROUP_ID)
def gwas_catalog_manifest_preparation():
    """Prepare initial manifest."""
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

    @task.branch(task_id="get_execution_mode")
    def get_execution_mode():
        """Get execution mode."""
        mode = get_full_config().config.mode.lower()
        return f"manifest_preparation.{mode}"

    execution_mode = get_execution_mode()

    @task(
        task_id="choose_manifest_paths",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def choose_manifest_paths(ti: TaskInstance | None = None) -> list[str]:
        """Choose manifests to pass to the next."""
        branch_name = ti.xcom_pull(task_ids="manifest_preparation.get_execution_mode")
        logging.info("BRANCH NAME: %s", branch_name)
        manifest_generation_task = f"{branch_name}.save_manifests"
        if branch_name == "manifest_preparation.resume":
            manifest_generation_task = "manifest_preparation.read_manifests"
        logging.info("MANIFEST GENERATION TASK: %s", manifest_generation_task)
        manifests = ti.xcom_pull(task_ids=manifest_generation_task)
        return [
            manifest["manifestPath"] for manifest in manifests if manifest["isCurated"]
        ]

    filtered_manifests = choose_manifest_paths()

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

    @task(task_id="get_all_sumstats")
    def get_all_sumstats(
        raw_sumstats_paths: list[str],
    ) -> dict[str, str]:
        """Get all sumstats."""
        return {extract_study_id_from_path(p): p for p in raw_sumstats_paths}

    @task(task_id="get_new_sumstats")
    def get_new_sumstats(
        raw_sumstats_paths: list[str],
        existing_manifest_paths: list[str],
    ) -> dict[str, str]:
        """Get new sumstats."""
        processed = {extract_study_id_from_path(p) for p in existing_manifest_paths}
        logging.info("ALREADY PROCESSED STUDIES: %s", len(processed))
        all = {extract_study_id_from_path(p): p for p in raw_sumstats_paths}
        logging.info("ALL STUDIES (INCLUDING NOT PROCESSED): %s", len(all))
        new = {key: val for key, val in all.items() if key not in processed}
        logging.info("NEW STUDIES UNPROCESSED: %s", len(new))
        return new

    @task(task_id="read_manifests")
    def read_manifests(manifest_paths: list[str]) -> list[Manifest_Object]:
        """Read manifests."""
        manifest_paths = [f"gs://{path}" for path in manifest_paths]
        return GCSIOManager().load_many(manifest_paths)

    for mode in ["force", "resume", "continue"]:
        branching_start = EmptyOperator(task_id=mode.lower())

        @task(task_id=f"{mode}.amend_curation_metadata")
        def amend_curation_metadata(new_manifests: list[Manifest_Object]):
            """Read curation file and add it to the partial manifests."""
            if new_manifests == []:
                return new_manifests
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

        @task(task_id=f"{mode}.genereate_new_manifests")
        def generate_new_manifests(
            new_sumstats: dict[str, str],
        ) -> list[Manifest_Object]:
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

        @task(task_id=f"{mode}.save_manifests")
        def save_manifests(manifests: list[Manifest_Object]) -> list[Manifest_Object]:
            """Write manifests to persistant storage."""
            manifest_paths = [manifest["manifestPath"] for manifest in manifests]
            logging.info("MANIFEST PATHS: %s", manifest_paths)
            GCSIOManager().dump_many(manifests, manifest_paths)
            return manifests

        if mode == "resume":
            manifests = read_manifests(existing_manifest_paths)
            chain(
                execution_mode,
                branching_start,
                manifests,
                filtered_manifests,
            )

        if mode == "force":
            new_sumstats = get_all_sumstats(raw_sumstats_paths)
            new_manifests = generate_new_manifests(new_sumstats)
            new_manifests_with_curation = amend_curation_metadata(new_manifests)
            manifests = save_manifests(new_manifests_with_curation)
            chain(
                execution_mode,
                branching_start,
                new_sumstats,
                new_manifests,
                new_manifests_with_curation,
                manifests,
                filtered_manifests,
            )
        if mode == "continue":

            @task.short_circuit(task_id=f"{mode}.exit_when_no_new_sumstats")
            def exit_when_no_new_sumstats(new_sumstats: dict[str, str]) -> bool:
                """Exit when no new sumstats."""
                logging.info("NEW SUMSTATS: %s", new_sumstats)
                return new_sumstats

            new_sumstats = get_new_sumstats(raw_sumstats_paths, existing_manifest_paths)
            new_manifests = generate_new_manifests(new_sumstats)
            new_manifests_with_curation = amend_curation_metadata(new_manifests)
            manifests = save_manifests(new_manifests_with_curation)
            no_new_sumstats = exit_when_no_new_sumstats(new_sumstats)
            chain(
                execution_mode,
                branching_start,
                new_sumstats,
                no_new_sumstats,
                new_manifests,
                new_manifests_with_curation,
                manifests,
                filtered_manifests,
            )

    chain(filtered_manifests, saved_config_path)


__all__ = ["gwas_catalog_manifest_preparation"]
