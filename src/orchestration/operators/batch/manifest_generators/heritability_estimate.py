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
from orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
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
    def from_generator_config(cls, specs: ManifestGeneratorSpecs) -> "HeritabilityManifestGenerator":
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
        """Build one batch job per study directory."""
        dataset_root = self.input_glob.gcs_path.rstrip("/")

        if not dataset_root.startswith("gs://"):
            raise ValueError(f"Expected gs:// path, got {dataset_root}")

        without_scheme = dataset_root[len("gs://") :]
        bucket_name, root_prefix = without_scheme.split("/", 1)
        root_prefix = root_prefix.rstrip("/") + "/"

        client = self.gcs_hook.get_conn()

        # Use delimiter="/" so GCS returns only top-level prefixes (study dirs)
        # rather than recursively listing every blob — critical at 100k+ studies.
        input_iter = client.list_blobs(bucket_name, prefix=root_prefix, delimiter="/")
        list(input_iter)  # consume iterator to populate .prefixes
        study_dirs = {p[len(root_prefix) :].rstrip("/") for p in (input_iter.prefixes or []) if p != root_prefix}

        # List existing outputs in a single API call instead of one per study.
        output_base = self.output_prefix.gcs_path.rstrip("/") + "/"
        out_without_scheme = output_base[len("gs://") :]
        out_bucket, out_prefix = out_without_scheme.split("/", 1)
        out_iter = client.list_blobs(out_bucket, prefix=out_prefix, delimiter="/")
        list(out_iter)
        existing_outputs = {p[len(out_prefix) :].rstrip("/") for p in (out_iter.prefixes or [])}

        vars_list: list[dict[str, str]] = []

        for study_dir in sorted(study_dirs):
            if study_dir in existing_outputs:
                continue

            vars_list.append({
                "INPUT_PARTITION": f"{dataset_root}/{study_dir}",
                "OUTPUT_PARTITION": f"{self.output_prefix.gcs_path.rstrip('/')}/{study_dir}",
            })

        print(f"dataset_root={dataset_root}")
        print(f"root_prefix={root_prefix}")
        print(f"n_study_dirs={len(study_dirs)}")
        print(f"n_existing_outputs={len(existing_outputs)}")
        print(f"study_dirs_sample={sorted(study_dirs)[:10]}")
        print(f"n_vars_list={len(vars_list)}")

        if not vars_list:
            raise AirflowSkipException(f"No study directories found to process under {dataset_root}")

        return vars_list
