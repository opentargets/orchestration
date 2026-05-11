"""Google Batch manifest generator for L2G prediction and shap explanation steps."""

from __future__ import annotations

from typing import Annotated

from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pydantic import BaseModel, StringConstraints

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.models.batch.environment import EnvironmentRegistrySpec, EnvironmentSpec
from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators.proto import ProtoManifestGenerator
from orchestration.utils.path import GCSPath


class GentropyStepManifestGeneratorOptions(BaseModel):
    """Specification for GentropyStepGoogleBatchManifestGenerator."""

    input_glob: Annotated[str, StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*/\*\*(\.[a-zA-Z0-9]+)+$")]
    """GCS glob pattern for input files. Example: gs://bucket_name/some/prefix/**.parquet"""
    output_prefix: Annotated[str, StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*/?$")] = ""
    """GCS prefix for output files. Example: gs://bucket_name/some/output/prefix"""


class GentropyStepManifestGenerator(ProtoManifestGenerator):
    def __init__(
        self,
        *,
        options: GentropyStepManifestGeneratorOptions,
        gcp_conn_id: str = "google_cloud_default",
    ):
        """Manifest generator for gentropy step running on google batch.

        This class should be utilized in case the gentropy step execution should be
        partitioned by arbitrary number of google batch tasks.

        Args:
            options (GentropyStepManifestGeneratorOptions): Options for the gentropy step.
            gcp_conn_id (str, optional): Google cloud connection. Defaults to "google_cloud_default".

        The `manifest_kwargs` represent the way to partition the input dataset. The default value provided should be
        {"input_glob": "gs://bucket_name/some/prefix/**.ext", "output_prefix": "gs://bucket_name/some/output/prefix"}.
        Depending on the number of files that match the `input_glob` the computed google batch job definition will have corresponding number of tasks.
        """
        self.options = options
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.input_glob = GCSPath(options.input_glob)
        self.output_prefix = GCSPath(options.output_prefix)

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> GentropyStepManifestGenerator:
        """Build Generator from generator specs."""
        return cls(
            options=GentropyStepManifestGeneratorOptions(**specs.generator_options),
        )

    def generate_batch_index(self) -> BatchIndex:
        """Generate index for google batch tasks."""
        return BatchIndex(env_registry=self._build_environment_registry())

    def _build_environment_registry(self) -> EnvironmentRegistrySpec:
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
        return EnvironmentRegistrySpec(
            environments=[
                EnvironmentSpec(
                    variables={
                        "INPUT_PARTITION": f"{protocol}://{bucket_name}/{file}",
                        "OUTPUT_PARTITION": f"{self.output_prefix.gcs_path}/{file.split('/')[-1]}",
                    }
                )
                for file in files
            ]
        )
