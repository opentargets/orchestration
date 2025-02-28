"""Google Batch manifest generator for L2G prediction and shap explanation steps."""

from __future__ import annotations

from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from ot_orchestration.operators.batch.batch_index import BatchIndex
from ot_orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from ot_orchestration.types import ManifestGeneratorSpecs
from ot_orchestration.utils.path import GCSPath


class L2GPredictionManifestGenerator(ProtoManifestGenerator):
    fields = {"credibleSetPartition": "CREDIBLE_SET_PARTITION", "predictionPartition": "PREDICTION_PARTITION"}

    def __init__(
        self,
        *,
        commands: list[str],
        options: dict[str, str],
        manifest_kwargs: dict[str, str],
        gcp_conn_id: str = "google_cloud_default",
    ):
        self.commands = commands
        self.options = options
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.cs_glob = GCSPath(manifest_kwargs["credible_set_glob"])

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpecs) -> L2GPredictionManifestGenerator:
        """Build Generator from generator specs."""
        return cls(
            commands=specs["commands"],
            options=specs["options"],
            manifest_kwargs=specs["manifest_kwargs"],
        )

    def generate_batch_index(self) -> BatchIndex:
        """Generate index for google batch tasks."""
        vars_list = self.build_vars_list()
        index = BatchIndex(
            vars_list=vars_list,
            options=self.options,
            commands=self.commands,
        )
        return index

    def build_vars_list(self) -> list[dict[str, str]]:
        """Build variable lists that will be later used to build google batch environments."""
        protocol = self.cs_glob.segments.get("protocol")
        bucket_name = self.cs_glob.segments.get("root")
        prefix = self.cs_glob.segments.get("prefix")
        match_glob = self.cs_glob.segments.get("filename")
        print(prefix, match_glob)
        files = self.gcs_hook.list(
            bucket_name=bucket_name,
            prefix=prefix + "/",
            match_glob=match_glob,
        )
        print(files)

        if len(files) == 0:
            raise AirflowSkipException(f"No credible set files found under {self.cs_glob} glob")
        return [{"cs_partition": f"{protocol}://{bucket_name}/{file}"} for file in files]
