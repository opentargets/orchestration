"""Airflow DAG for GWAS Catalog processing."""

from __future__ import annotations

from airflow.utils.context import Context
from ot_orchestration.utils import read_yaml_config
from ot_orchestration.utils.common import shared_dag_args, shared_dag_kwargs
from airflow.models.dag import DAG
from pathlib import Path
from airflow.decorators import task
from airflow.operators.python import get_current_context
import logging
from airflow.models.baseoperator import BaseOperator
from typing import Sequence
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from airflow.models.taskinstance import TaskInstance
from airflow.models.baseoperator import chain
from google.cloud.batch_v1 import Environment
import re
from ot_orchestration.utils.manifest import extract_study_id_from_path
from ot_orchestration.types import Manifest_Object
from ot_orchestration.utils.batch import create_batch_job, create_task_spec
from ot_orchestration.utils import IOManager
import time
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

config_path = "/opt/airflow/config/configv2.yaml"
config = read_yaml_config(config_path)
logging.basicConfig(level=logging.INFO)


class ManifestGenerateOperator(BaseOperator):
    """Generate manifest dictionary for each summary statistics."""

    template_fields: Sequence[str] = (
        "raw_sumstat_path_pattern",
        "staging_manifest_path_pattern",
        "harmonised_prefix",
        "qc_prefix",
    )

    def __init__(
        self,
        *,
        raw_sumstat_path_pattern: str,
        staging_manifest_path_pattern: str,
        harmonised_prefix: str = "harmonised",
        qc_prefix: str = "qc",
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.raw_sumstat_path_pattern = raw_sumstat_path_pattern
        self.staging_manifest_path_pattern = staging_manifest_path_pattern
        self.harmonised_prefix = harmonised_prefix
        self.qc_prefix = qc_prefix
        self.gcp_conn_id = gcp_conn_id

    def execute(self, **_) -> list[Manifest_Object]:
        """Execute the operator.

        Raises:
            ValueError: when incorrect glob is defined
            ValueError: when the glob protocol is not gs

        Returns:
            dict[str, str]: list of manifests
        """
        # this regex pattern can be utilized for any path or uri glob pattern
        pattern = r"^((?P<protocol>.*)://)?(?P<root>[(\w)-]+)/(?P<prefix>([(\w)-/])+?)/(?P<matchglob>[(\w)-*]+.*){1}"
        compiled_pattern = re.compile(pattern)

        globs = {
            "raw_sumstat": self.raw_sumstat_path_pattern,
            "manifest": self.staging_manifest_path_pattern,
        }

        results = {}
        for key, glob in globs.items():
            _match = compiled_pattern.match(glob)
            if _match is None:
                raise ValueError("Incorrect glob pattern %s", glob)
            protocol = _match.group("protocol")
            root = _match.group("root")
            prefix = _match.group("prefix")
            matchglob = _match.group("matchglob")

            if protocol != "gs":
                raise NotImplementedError(
                    "Listing objects from path with %s protocol is not implemented",
                    protocol,
                )
            logging.info(
                "Listing files at %s/%s with match glob %s", root, prefix, matchglob
            )
            files = GCSHook(gcp_conn_id=self.gcp_conn_id).list(
                bucket_name=root,
                prefix=prefix,
                match_glob=matchglob,
            )
            logging.info("Found %s %s files", len(files), key)
            logging.info(files)
            results[key] = {
                "common_path": f"{protocol}://{root}/{prefix}",
                "samplesheet": {
                    extract_study_id_from_path(s): f"{protocol}://{root}/{s}"
                    for s in files
                },
            }

        studies_with_sumstats: dict[str, str] = results["raw_sumstat"]["samplesheet"]
        studies_with_manifests: dict[str, str] = results["manifest"]["samplesheet"]
        new_study_ids = set(studies_with_sumstats.keys()) - set(
            studies_with_manifests.keys()
        )
        common_path: str = results["manifest"]["common_path"]
        # generate the manifest per each new sumstat
        manifests = []
        for study_id, raw_sumstat_path in studies_with_sumstats.items():
            if study_id in new_study_ids:
                parital_manifest = {
                    "studyId": study_id,
                    "rawPath": raw_sumstat_path,
                    "manifestPath": f"{common_path}/{study_id}/manifest.json",
                    "harmonisedPath": f"{common_path}/{study_id}/{self.harmonised_prefix}",
                    "qcPath": f"{common_path}/{study_id}/{self.qc_prefix}",
                    "passHarmonisation": False,
                    "passQc": False,
                }
                manifests.append(parital_manifest)

        return manifests


class ManifestSaveOperator(BaseOperator):
    """Save manifest dictionaries to files."""

    template_fields: Sequence[str] = ["manifest_blobs"]

    def __init__(self, manifest_blobs: list[Manifest_Object], **kwargs) -> None:
        super().__init__(**kwargs)
        self.manifest_blobs = manifest_blobs

    def execute(self, **_) -> list[Manifest_Object]:
        """Execute the operator.

        Returns:
            list[Manifest_Object]: saved manifests
        """
        logging.info(self.manifest_blobs)
        manifest_paths = [m["manifestPath"] for m in self.manifest_blobs]
        IOManager().dump_many(self.manifest_blobs, manifest_paths)
        return self.manifest_blobs


class ManifestReadOperator(BaseOperator):
    """Read manifest json files."""

    template_fields: Sequence[str] = ["staging_manifest_path_pattern"]

    def __init__(
        self,
        staging_manifest_path_pattern: str,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.staging_manifest_path_pattern = staging_manifest_path_pattern
        self.gcp_conn_id = gcp_conn_id

    def execute(self, **_) -> list[Manifest_Object]:
        """Read manifests.

        Raises:
            ValueError: when incorrect glob is defined.
            NotImplementedError: for protocol other then gs.

        Returns:
            list[Manifest_Object]: list of read manifests.
        """
        logging.info(self.staging_manifest_path_pattern)
        pattern = r"^((?P<protocol>.*)://)?(?P<root>[(\w)-]+)/(?P<prefix>([(\w)-/])+?)/(?P<matchglob>[(\w)-*]+.*){1}"
        compiled_pattern = re.compile(pattern)
        _match = compiled_pattern.match(self.staging_manifest_path_pattern)
        if _match is None:
            raise ValueError(
                "Incorrect glob pattern %s", self.staging_manifest_path_pattern
            )
        protocol = _match.group("protocol")
        root = _match.group("root")
        prefix = _match.group("prefix")
        matchglob = _match.group("matchglob")

        if protocol != "gs":
            raise NotImplementedError(
                "Listing objects from path with %s protocol is not implemented",
                protocol,
            )
        logging.info(
            "Listing files at %s/%s with match glob %s", root, prefix, matchglob
        )
        manifest_paths = GCSHook(gcp_conn_id=self.gcp_conn_id).list(
            bucket_name=root,
            prefix=prefix,
            match_glob=matchglob,
        )
        manifest_paths = [f"{protocol}://{root}/{mp}" for mp in manifest_paths]
        manifests = IOManager().load_many(manifest_paths)
        return manifests


class ManifestSubmitBatchJobOperator(BaseOperator):
    """Submit manifest as a batch job."""

    template_fields: Sequence[str] = ["job_name", "manifests", "step"]

    def __init__(
        self, step: str, job_name: str, manifests: list[Manifest_Object], **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.kwargs = kwargs
        self.manifests = manifests
        self.step = step

    def execute(self, context: Context) -> str:
        """Execute the operator.

        Args:
            context (Context): execution context

        Raises:
            AirflowSkipException: when no manifest files are found

        Returns:
            str: google batch job name.
        """
        # in case no manifest files, do not create the batch job
        manifest_paths = [m["manifestPath"] for m in self.manifests]

        if not manifest_paths:
            raise AirflowSkipException("No manifests to run the batch job")
        params = context.get("params")
        gcp = params.get("gcp")
        gcp_project = gcp.get("GCP_PROJECT")
        gcp_region = gcp.get("GCP_REGION")
        steps_params = params.get("steps")
        step_params = steps_params.get(self.step)
        google_batch_specs = step_params.get("googlebatch")
        policy_specs = google_batch_specs.get("policy_specs")
        resource_specs = google_batch_specs.get("resource_specs")
        task_specs = google_batch_specs.get("task_specs")
        image = google_batch_specs.get("image")
        commands = google_batch_specs.get("commands")
        task_spec = create_task_spec(image, commands, resource_specs, task_specs)
        task_env = [
            Environment(variables={"MANIFEST_PATH": mp}) for mp in manifest_paths
        ]
        batch_job = create_batch_job(task_spec, task_env, policy_specs)
        logging.info(batch_job)
        self.task_id
        cloudbatch_operator = CloudBatchSubmitJobOperator(
            project_id=gcp_project,
            region=gcp_region,
            job_name=self.job_name,
            job=batch_job,
            deferrable=False,
            **self.kwargs,
        )
        cloudbatch_operator.execute(context)
        return self.job_name


class ManifestFilterOperator(BaseOperator):
    """Filter manifests based on the previous task status.

    The operator filters the manifests based on the `pass*` flags and returns
    the set of manifests where any flag is False.
    """

    template_fields: Sequence[str] = ["manifests"]

    def __init__(self, manifests: list[Manifest_Object], **kwargs) -> None:
        super().__init__(**kwargs)
        self.manifests = manifests

    def execute(self, **_) -> list[Manifest_Object]:
        """Execute the operator."""
        step_flag_prefix = "pass"
        filtered_manifests = []
        for manifest in self.manifests:
            for key, val in manifest.items():
                if key.startswith(step_flag_prefix) and not val:
                    filtered_manifests.append(manifest)
        logging.info("PREVIOUSLY FAILED TASKS: %s", len(filtered_manifests))
        return filtered_manifests


@task(
    task_id="consolidate_manifests",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
def consolidate_manifests(ti: TaskInstance | None = None) -> list[Manifest_Object]:
    """Consolidate manifests from the execution mode branching."""
    params = get_current_context().get("params")
    if params["mode"] == "CONTINUE":
        branch = "generate_staging_output"
    else:
        branch = "filter_failed_manifests"
    manifests = ti.xcom_pull(branch)
    if len(manifests) == 0:
        raise AirflowSkipException("No manifests detected, skipping.")
    return manifests


@task.branch(task_id="begin")
def begin() -> str:
    """Start the DAG execution by choosing the execution mode."""
    logging.info("START")
    params = get_current_context().get("params")
    logging.info(params)
    if params["mode"] == "CONTINUE":
        return "generate_manifests"
    else:
        return "read_existing_manifests"


@task(task_id="collect_task_outcome", multiple_outputs=True)
def collect_task_outcome(manifests: list[Manifest_Object]):
    """Collect the task(s) outcome and return failed and succeded manifests."""
    # we need to re-read the manifests to report the updated status of the tasks
    manifest_paths = [m["manifestPath"] for m in manifests]
    new_manifests: list[Manifest_Object] = IOManager().load_many(manifest_paths)

    failed_manifests = []
    succeded_manifests = []
    status_flag_prefix = "pass"
    for new_manifest in new_manifests:
        # we assume that the initial state is success
        succeded = True
        for key, val in new_manifest.items():
            if key.startswith(status_flag_prefix) and not val:
                succeded = False

        if succeded:
            succeded_manifests.append(new_manifest)
        else:
            failed_manifests.append(new_manifest)

    logging.info("FAILED MANIFESTS %s/%s", len(failed_manifests), len(manifests))
    return {
        "failed_manifests": failed_manifests,
        "succeded_manifests": succeded_manifests,
    }


@task(task_id="end")
def end():
    """Finish the DAG execution."""
    logging.info("FINISHED")


with DAG(
    dag_id=Path(__file__).stem,
    description="Open Targets Genetics - GWAS Catalog processing",
    params=config,
    default_args=shared_dag_args,
    **shared_dag_kwargs,
) as dag:
    exec_mode_branch = begin()

    new_manifests = ManifestGenerateOperator(
        task_id="generate_manifests",
        raw_sumstat_path_pattern="{{ params.steps.manifest_preparation.raw_sumstat_path_pattern }}",
        staging_manifest_path_pattern="{{ params.steps.manifest_preparation.staging_manifest_path_pattern }}",
        harmonised_prefix="{{ params.steps.manifest_preparation.harmonised_prefix }}",
        qc_prefix="{{ params.steps.manifest_preparation.qc_prefix }}",
    ).output

    existing_manifests = ManifestReadOperator(
        task_id="read_existing_manifests",
        staging_manifest_path_pattern="{{ params.steps.manifest_preparation.staging_manifest_path_pattern }}",
    ).output

    failed_existing_manifests = ManifestFilterOperator(
        task_id="filter_failed_manifests",
        manifests=existing_manifests,
    ).output

    saved_manifests = ManifestSaveOperator(
        task_id="generate_staging_output",
        manifest_blobs=new_manifests,  # type: ignore
    ).output

    consolidated_manifests = consolidate_manifests()
    batch_job = ManifestSubmitBatchJobOperator(
        task_id="gwas-catalog_batch_job",
        manifests=consolidated_manifests,  # type: ignore
        job_name=f"gwas-catalog-job-{time.strftime('%Y%m%d-%H%M%S')}",
        step="gwas-catalog-etl",
    ).output

    updated_manifests = collect_task_outcome(manifests=consolidated_manifests)

    # MODE == CONTINUE
    chain(
        exec_mode_branch,
        new_manifests,
        saved_manifests,
        consolidated_manifests,
    )

    # MODE == RESUME
    chain(
        exec_mode_branch,
        existing_manifests,
        failed_existing_manifests,
        consolidated_manifests,
    )

    # ALWAYS AFTER
    chain(
        consolidated_manifests,
        batch_job,
        updated_manifests,
        end(),
    )
