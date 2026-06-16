"""Google Batch manifest generator for heritability estimation step."""

from __future__ import annotations

import logging
from typing import Annotated

from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pydantic import BaseModel, StringConstraints

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.models.batch.environment import EnvironmentRegistrySpec, EnvironmentSpec
from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators.proto import ProtoManifestGenerator
from orchestration.utils.path import GCSPath

logger = logging.getLogger(__name__)


class HeritabilityManifestGeneratorOptions(BaseModel):
    """Options for the heritability manifest generator."""

    input_glob: Annotated[
        str,
        StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)+$"),
    ]
    """GCS URI of a GCS directory containing per-study harmonised summary statistics sub-directories.

    Must be a non-empty path (at least ``gs://bucket/prefix``). The generator uses this to scan for
    study sub-directories.
    """

    output_prefix: Annotated[
        str,
        StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*/?$|^$"),
    ] = ""
    """GCS prefix under which per-study heritability outputs will be written.

    Defaults to an empty string (which means output goes directly under the bucket root). Must be a
    valid GCS URI with any number of path segments, or empty.
    """


class HeritabilityManifestGenerator(ProtoManifestGenerator):
    """Manifest generator for heritability estimation.

    Scans ``input_glob`` for study sub-directories and pairs each one with a
    corresponding output path under ``output_prefix``.  Studies whose output
    directory already exists are skipped.
    """

    def __init__(
        self,
        *,
        options: HeritabilityManifestGeneratorOptions,
        gcp_conn_id: str = "google_cloud_default",
    ) -> None:
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.input_glob = GCSPath(options.input_glob)
        self.output_prefix = GCSPath(options.output_prefix)

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> HeritabilityManifestGenerator:
        """Construct a generator from a ``ManifestGeneratorSpec``."""
        return cls(options=HeritabilityManifestGeneratorOptions(**specs.generator_options))

    def generate_batch_index(self) -> BatchIndex:
        """Build the batch index from the per-study environment registry."""
        vars_list = self.build_vars_list()
        env_registry = EnvironmentRegistrySpec(environments=[EnvironmentSpec(variables=row) for row in vars_list])
        return BatchIndex(env_registry=env_registry)

    def build_vars_list(self) -> list[dict[str, str]]:
        """Return one INPUT_PARTITION/OUTPUT_PARTITION pair per unprocessed study."""
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

        vars_list = [
            {
                "INPUT_PARTITION": f"{dataset_root}/{study_dir}",
                "OUTPUT_PARTITION": f"{self.output_prefix.gcs_path.rstrip('/')}/{study_dir}",
            }
            for study_dir in sorted(study_dirs)
            if study_dir not in existing_outputs
        ]

        logger.info(
            "heritability manifest scan complete: %d study dirs found, %d with existing outputs, %d to process",
            len(study_dirs),
            len(existing_outputs),
            len(vars_list),
        )

        if not vars_list:
            raise AirflowSkipException(f"No study directories found to process under {dataset_root}")

        return vars_list
