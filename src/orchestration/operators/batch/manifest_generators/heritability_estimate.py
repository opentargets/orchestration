"""Google Batch manifest generator for heritability estimation step.

This manifest generator is responsible for preparing the input/output
environment for running the heritability estimation step across many
harmonised summary statistics.  It operates by scanning a glob of
harmonised summary statistics on Google Cloud Storage (GCS) and
constructing a per‑file mapping into a corresponding heritability
estimate output path.  If the heritability estimate for a given
summary statistic already exists, the file is skipped.  The result is
used by the ``BatchIndexOperator`` to build individual Google Batch
tasks.

The generator mirrors the behaviour of
``GentropyStepGoogleBatchManifestGenerator`` used for the locus‑to‑gene
prediction step but adds an existence check on the output path.  This
avoids re‑computing heritability estimates for studies which have
already been processed.
"""

from __future__ import annotations

from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators import \
    ProtoManifestGenerator
from orchestration.types import ManifestGeneratorSpecs
from orchestration.utils.path import GCSPath


class HeritabilityManifestGenerator(ProtoManifestGenerator):
    """Manifest generator for heritability estimation.

    Parameters
    ----------
    commands : list[str]
        Shell command fragments used when invoking the gentropy CLI.
    options : dict[str, str]
        Hydra options controlling the gentropy step.  These options are
        propagated unchanged into the batch job environment.
    manifest_kwargs : dict[str, str]
        A mapping containing the keys ``input_glob`` and ``output_prefix``.
        ``input_glob`` should be a GCS URI glob pointing at the
        harmonised summary statistics.  ``output_prefix`` is the base
        directory under which heritability outputs will be written.
    gcp_conn_id : str, optional
        Airflow connection ID used for the GCS client.  Defaults to
        ``"google_cloud_default"``.
    """

    def __init__(
        self,
        *,
        commands: list[str],
        options: dict[str, str],
        manifest_kwargs: dict[str, str],
        gcp_conn_id: str = "google_cloud_default",
    ) -> None:
        self.commands = commands
        self.options = options
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.input_glob = GCSPath(manifest_kwargs.get("input_glob", ""))
        self.output_prefix = GCSPath(manifest_kwargs.get("output_prefix", ""))

    @classmethod
    def from_generator_config(
        cls, specs: ManifestGeneratorSpecs
    ) -> "HeritabilityManifestGenerator":
        """Construct a generator from configuration specs.

        This method is invoked by the ``BatchIndexOperator`` when
        deserialising the manifest generator from the YAML config.  It
        forwards the specification fields directly to the constructor.
        """
        return cls(
            commands=specs["commands"],
            options=specs["options"],
            manifest_kwargs=specs["manifest_kwargs"],
        )

    def generate_batch_index(self) -> BatchIndex:
        """Create the batch index used by Google Batch.

        The index comprises a list of dictionaries, each containing
        ``INPUT_PARTITION`` and ``OUTPUT_PARTITION`` environment
        variables.  These entries feed into the batch job to drive
        partitioned processing of individual summary statistics files.
        """
        vars_list = self.build_vars_list()
        return BatchIndex(
            vars_list=vars_list,
            options=self.options,
            commands=self.commands,
        )

    def build_vars_list(self) -> list[dict[str, str]]:
        """Build the list of variables for each batch task.

        This method enumerates all files that match the input glob.  For
        each input file it derives a corresponding output path under
        ``output_prefix`` using the basename of the file.  Before
        adding a task entry, it checks whether the output file already
        exists on GCS and skips it if present.  If no new tasks are
        required, an ``AirflowSkipException`` is raised to short‑circuit
        job creation.

        Returns
        -------
        list[dict[str, str]]
            A list of environment variable mappings used by
            ``BatchIndexOperator``.
        """
        protocol = self.input_glob.segments.get("protocol")
        bucket_name = self.input_glob.segments.get("root")
        prefix = self.input_glob.segments.get("prefix")
        match_glob = self.input_glob.segments.get("filename")

        # Retrieve all candidate files from the input glob.
        files = self.gcs_hook.list(
            bucket_name=bucket_name,
            prefix=f"{prefix}/" if prefix else "",
            match_glob=match_glob,
        )

        vars_list: list[dict[str, str]] = []
        for file in files:
            # Construct full GCS path for the input partition.
            input_path = f"{protocol}://{bucket_name}/{file}"
            # Derive the output filename from the last path segment of the
            # input file to avoid recreating the entire directory structure.
            output_filename = file.split("/")[-1]
            output_path = f"{self.output_prefix.gcs_path}/{output_filename}"

            # Skip processing if the output already exists.
            output_gcs = GCSPath(output_path)
            try:
                exists = output_gcs.exists()
            except Exception:
                # In case the existence check fails due to network or
                # authentication errors, pessimistically assume the file
                # does not exist so it will be regenerated.
                exists = False
            if exists:
                continue
            vars_list.append(
                {
                    "INPUT_PARTITION": input_path,
                    "OUTPUT_PARTITION": output_path,
                }
            )

        if not vars_list:
            raise AirflowSkipException(
                f"No files to process under {self.input_glob.gcs_path}; "
                "all heritability estimates appear to exist."
            )
        return vars_list        return vars_list