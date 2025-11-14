"""Google Batch manifest generator for L2G prediction and shap explanation steps."""

from __future__ import annotations

from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from orchestration.types import ManifestGeneratorSpecs
from orchestration.utils.path import GCSPath


class GentropyStepGoogleBatchManifestGenerator(ProtoManifestGenerator):
    def __init__(
        self,
        *,
        commands: list[str],
        options: dict[str, str],
        manifest_kwargs: dict[str, str],
        gcp_conn_id: str = "google_cloud_default",
    ):
        """Manifest generator for gentropy step running on google batch.

        This class should be utilized in case the gentropy step execution should be
        partitioned by arbitrary number of google batch tasks.

        Args:
            commands (list[str]): List of commands to run the gentropy step.
            options (dict[str, str]): dictionary of options to run the step. Typically these are {"step": "l2g_prediction"}.
            manifest_kwargs (dict[str, str]): Arguments used to derive the batch job partitioning.
            gcp_conn_id (str, optional): Google cloud connection. Defaults to "google_cloud_default".

        The `manifest_kwargs` represent the way to partition the input dataset. The default value provided should be
        {"input_glob": "gs://bucket_name/some/prefix/**.ext"}. Depending on the number of files that match the `input_glob`
        the computed google batch job definition will have corresponding number of tasks.
        """
        self.commands = commands
        self.options = options
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.input_glob = GCSPath(manifest_kwargs["input_glob"])

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpecs) -> GentropyStepGoogleBatchManifestGenerator:
        """Build Generator from generator specs."""
        return cls(
            commands=specs["commands"],
            options=specs["options"],
            manifest_kwargs=specs["manifest_kwargs"],
        )

    def generate_batch_index(self) -> BatchIndex:
        """Generate index for google batch tasks."""
        vars_list = self.build_vars_list()
        return BatchIndex(
            vars_list=vars_list,
            options=self.options,
            commands=self.commands,
        )

    def build_vars_list(self) -> list[dict[str, str]]:
        """Build variable lists that will be later used to build google batch environments."""
        protocol = self.input_glob.segments.get("protocol")
        bucket_name = self.input_glob.segments.get("root")
        prefix = self.input_glob.segments.get("prefix")
        match_glob = self.input_glob.segments.get("filename")
        files = self.gcs_hook.list(
            bucket_name=bucket_name,
            prefix=prefix + "/",
            match_glob=match_glob,
        )

        if len(files) == 0:
            raise AirflowSkipException(f"No files found under {self.input_glob} glob")
        return [{"input_partition": f"{protocol}://{bucket_name}/{file}"} for file in files]
