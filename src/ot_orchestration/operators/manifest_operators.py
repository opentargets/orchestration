"""Manifest operators."""

from functools import cached_property
from typing import Any, Sequence

from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from airflow.utils.context import Context
from google.cloud.batch_v1 import Environment

from ot_orchestration.types import ManifestObject
from ot_orchestration.utils.batch import create_batch_job, create_task_spec
from ot_orchestration.utils.manifest import extract_study_id_from_path
from ot_orchestration.utils.path import IOManager


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
        self.status = "pending"

    @cached_property
    def gcs_hook(self) -> GCSHook:
        """Get the google cloud storage hook."""
        return GCSHook(gcp_conn_id=self.gcp_conn_id)

    @cached_property
    def io_manager(self) -> IOManager:
        """Get IO manager."""
        return IOManager()

    def execute(self, **kwargs: dict[str, Any]) -> list[ManifestObject]:
        """Execute the operator.

        Arguments:
            **kwargs(dict[str, Any]): Keyword arguments provided by BaseOperator signature. Not used.

        Raises:
            ValueError: when the glob protocol is not gs

        Returns:
            list[ManifestObject]: list of manifests
        """
        globs = {
            "raw_sumstat": self.raw_sumstat_path_pattern,
            "manifest": self.staging_manifest_path_pattern,
        }

        results = {}
        for key, glob in globs.items():
            path = self.io_manager.resolve(glob)
            protocol = path.segments.get("protocol")
            root = path.segments.get("root")
            prefix = path.segments.get("prefix")
            matchglob = path.segments.get("filename")

            if protocol != "gs":
                raise NotImplementedError(
                    "Listing objects from path with %s protocol is not implemented",
                    protocol,
                )
            self.log.info(
                "Listing files at %s/%s with match glob %s", root, prefix, matchglob
            )
            files = self.gcs_hook.list(
                bucket_name=root,
                prefix=prefix,
                match_glob=matchglob,
            )
            self.log.info("Found %s %s files", len(files), key)
            self.log.info(files)
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
        if len(new_study_ids) == 0:
            raise AirflowSkipException("All raw sumstats have manifests, skipping.")
        common_path: str = results["manifest"]["common_path"]
        # generate the manifest per each new sumstat
        manifests = []
        for study_id, raw_sumstat_path in studies_with_sumstats.items():
            if study_id in new_study_ids:
                partial_manifest = {
                    "studyId": study_id,
                    "rawPath": raw_sumstat_path,
                    "manifestPath": f"{common_path}/{study_id}/manifest.json",
                    "harmonisedPath": f"{common_path}/{study_id}/{self.harmonised_prefix}",
                    "qcPath": f"{common_path}/{study_id}/{self.qc_prefix}",
                    "passHarmonisation": False,
                    "passQc": False,
                    "status": self.status,
                }
                manifests.append(partial_manifest)

        return manifests


class ManifestSaveOperator(BaseOperator):
    """Save manifest dictionaries to files."""

    template_fields: Sequence[str] = ["manifest_blobs"]

    def __init__(self, manifest_blobs: list[ManifestObject], **kwargs) -> None:
        super().__init__(**kwargs)
        self.manifest_blobs = manifest_blobs

    def execute(self, **kwargs: dict[str, Any]) -> list[ManifestObject]:
        """Execute the operator.

        Arguments:
            **kwargs(dict[str, Any]): Keyword arguments provided by BaseOperator signature. Not used.

        Returns:
            list[ManifestObject]: saved manifests
        """
        self.log.info(self.manifest_blobs)
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

    @cached_property
    def io_manager(self) -> IOManager:
        """Get IO manager."""
        return IOManager()

    @cached_property
    def gcs_hook(self) -> GCSHook:
        """Get the google cloud storage hook."""
        return GCSHook(gcp_conn_id=self.gcp_conn_id)

    def execute(self, **kwargs: dict[str, Any]) -> list[ManifestObject]:
        """Read manifests.

        Arguments:
            **kwargs(dict[str, Any]): Keyword arguments provided by BaseOperator signature. Not used.

        Raises:
            ValueError: when incorrect glob is defined.
            NotImplementedError: for protocol other then gs.

        Returns:
            list[ManifestObject]: list of read manifests.
        """
        self.log.info("Reading manifests from: %s", self.staging_manifest_path_pattern)
        path = self.io_manager.resolve(self.staging_manifest_path_pattern)
        protocol = path.segments.get("protocol")
        root = path.segments.get("root")
        prefix = path.segments.get("prefix")
        matchglob = path.segments.get("filename")

        if protocol != "gs":
            raise NotImplementedError(
                "Listing objects from path with %s protocol is not implemented",
                protocol,
            )
        self.log.info(
            "Listing files at %s/%s with match glob %s", root, prefix, matchglob
        )
        manifest_paths = self.gcs_hook.list(
            bucket_name=root,
            prefix=prefix,
            match_glob=matchglob,
        )
        manifest_paths = [f"{protocol}://{root}/{mp}" for mp in manifest_paths]
        manifests = IOManager().load_many(manifest_paths)
        return manifests


class ManifestSubmitBatchJobOperator(BaseOperator):
    """Submit manifest as a batch job."""

    template_fields: Sequence[str] = [
        "job_name",
        "manifests",
        "step",
        "gcp_project",
        "gcp_region",
    ]

    def __init__(
        self,
        step: str,
        job_name: str,
        manifests: list[ManifestObject],
        gcp_project: str,
        gcp_region: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.kwargs = kwargs
        self.manifests = manifests
        self.step = step
        self.gcp_project = gcp_project
        self.gcp_region = gcp_region

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
            raise AirflowSkipException("No manifests to run the batch job.")
        params = context.get("params")
        if not params:
            raise AirflowSkipException("No params found for batch job.")
        steps_params = params.get("steps")
        if not steps_params:
            raise AirflowSkipException("No params found for steps.")
        step_params = steps_params.get(self.step)
        if not step_params:
            raise AirflowSkipException(f"No params for step {self.step} were found.")
        google_batch_job_specs = step_params.get("googlebatch")
        if not google_batch_job_specs:
            raise AirflowSkipException(
                f"No batch job params were defined for step {self.step}."
            )
        policy_specs = google_batch_job_specs.get("policy_specs")
        if not policy_specs:
            raise AirflowSkipException(
                f"No policy specs defined found for step {self.step} batch job configuration."
            )
        resource_specs = google_batch_job_specs.get("resource_specs")
        if not resource_specs:
            raise AirflowSkipException(
                f"No resource specs defined found for step {self.step} batch job configuration."
            )
        task_specs = google_batch_job_specs.get("task_specs")
        if not task_specs:
            raise AirflowSkipException(
                f"No task specs were defined for step {self.step} batch job configuration."
            )
        image = google_batch_job_specs.get("image")
        if not policy_specs:
            raise AirflowSkipException(
                f"No image was defined for step {self.step} batch job configuration."
            )
        commands = google_batch_job_specs.get("commands")
        if not commands:
            raise AirflowSkipException(
                f"No commands were defined for step {self.step} batch job configuration."
            )
        task_spec = create_task_spec(image, commands, resource_specs, task_specs)
        task_env = [
            Environment(variables={"MANIFEST_PATH": mp}) for mp in manifest_paths
        ]
        batch_job = create_batch_job(task_spec, task_env, policy_specs)
        self.log.info(batch_job)
        self.task_id
        cloudbatch_operator = CloudBatchSubmitJobOperator(
            project_id=self.gcp_project,
            region=self.gcp_region,
            job_name=self.job_name,
            job=batch_job,
            deferrable=False,
            **self.kwargs,
        )
        cloudbatch_operator.execute(context)
        return self.job_name


class ManifestFilterOperator(BaseOperator):
    """Filter manifests based on the previous task status.

    The operator filters the manifests based on the `pass*` flags and
    returns the set of manifests where any flag is False.
    """

    template_fields: Sequence[str] = ["manifests"]

    def __init__(self, manifests: list[ManifestObject], **kwargs) -> None:
        super().__init__(**kwargs)
        self.manifests = manifests

    def execute(self, **_) -> list[ManifestObject]:
        """Execute the operator."""
        step_flag_prefix = "pass"
        filtered_manifests = []
        for manifest in self.manifests:
            for key, val in manifest.items():
                if key.startswith(step_flag_prefix) and not val:
                    filtered_manifests.append(manifest)
        self.log.info("PREVIOUSLY FAILED TASKS: %s", len(filtered_manifests))
        return filtered_manifests
