"""Google Batch manifest generator for the genetic correlation pipeline.

Reads the all_pairs.csv manifest produced by RepresentativeStudyManifestStep,
splits it into fixed-size chunk CSV files, uploads them to GCS, and returns
one BatchIndexRow per unprocessed chunk.

Each Batch task receives:
  INPUT_PARTITION  — gs:// path to the per-chunk CSV (studyId_1, studyId_2, ancestry)
  OUTPUT_PARTITION — gs:// path where the chunk's result parquet should be written
"""

from __future__ import annotations

import io
import logging

import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pydantic import BaseModel, Field

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.models.batch.environment import EnvironmentRegistrySpec, EnvironmentSpec
from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators.proto import ProtoManifestGenerator

logger = logging.getLogger(__name__)


class GeneticCorrelationManifestGeneratorOptions(BaseModel):
    """Options for the genetic correlation manifest generator."""

    manifest_path: str
    """GCS URI of the all_pairs.csv produced by RepresentativeStudyManifestStep."""

    output_prefix: str
    """GCS prefix under which per-chunk result parquets will be written.

    Chunk outputs land at ``{output_prefix}/chunk_{idx:05d}/``.
    """

    chunks_staging_path: str
    """GCS prefix where per-chunk input CSV files are staged.

    Chunk CSVs land at ``{chunks_staging_path}/chunk_{idx:05d}.csv``.
    """

    chunk_size: int = Field(default=200, gt=0)
    """Number of study pairs to process per Batch task (default 200)."""


class GeneticCorrelationManifestGenerator(ProtoManifestGenerator):
    """Manifest generator for the genetic correlation batch pipeline.

    Reads ``all_pairs.csv`` from GCS, splits it into chunks of ``chunk_size`` pairs,
    uploads each chunk as a CSV to ``chunks_staging_path``, and returns one
    environment spec per chunk that has not yet produced output under ``output_prefix``.
    Chunks whose output directory already exists are skipped (resumable).
    """

    def __init__(
        self,
        *,
        options: GeneticCorrelationManifestGeneratorOptions,
        gcp_conn_id: str = "google_cloud_default",
    ) -> None:
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.options = options

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> GeneticCorrelationManifestGenerator:
        """Construct a generator from a ``ManifestGeneratorSpec``."""
        return cls(options=GeneticCorrelationManifestGeneratorOptions(**specs.generator_options))

    def generate_batch_index(self) -> BatchIndex:
        """Build the batch index from chunk environment specs."""
        vars_list = self.build_vars_list()
        env_registry = EnvironmentRegistrySpec(
            environments=[EnvironmentSpec(variables=row) for row in vars_list]
        )
        return BatchIndex(env_registry=env_registry)

    def build_vars_list(self) -> list[dict[str, str]]:
        """Return one INPUT/OUTPUT pair per unprocessed chunk."""
        client = self.gcs_hook.get_conn()

        # Download and parse the pairs manifest
        pairs_df = self._download_manifest(client)
        n_total = len(pairs_df)
        logger.info("Pairs manifest: %d pairs total", n_total)

        # Chunk into fixed-size groups
        chunk_size = self.options.chunk_size
        chunks = [
            pairs_df.iloc[i : i + chunk_size]
            for i in range(0, n_total, chunk_size)
        ]
        n_chunks = len(chunks)
        logger.info("Splitting into %d chunks of up to %d pairs", n_chunks, chunk_size)

        # List already-completed output directories
        existing = self._list_existing_chunks(client)
        logger.info("%d chunks already completed", len(existing))

        output_prefix = self.options.output_prefix.rstrip("/")
        chunks_base = self.options.chunks_staging_path.rstrip("/")
        vars_list: list[dict[str, str]] = []

        for idx, chunk_df in enumerate(chunks):
            chunk_name = f"chunk_{idx:05d}"
            if chunk_name in existing:
                continue

            chunk_csv_path = f"{chunks_base}/{chunk_name}.csv"
            self._upload_chunk(client, chunk_df, chunk_csv_path)

            vars_list.append({
                "INPUT_PARTITION": chunk_csv_path,
                "OUTPUT_PARTITION": f"{output_prefix}/{chunk_name}",
            })

        logger.info(
            "genetic_correlation manifest: %d/%d chunks to process",
            len(vars_list),
            n_chunks,
        )

        if not vars_list:
            raise AirflowSkipException(
                f"All {n_chunks} chunks already completed under {output_prefix}"
            )

        return vars_list

    # ── private helpers ───────────────────────────────────────────────────────

    def _download_manifest(self, client) -> pd.DataFrame:
        """Download and parse the pairs manifest CSV from GCS."""
        bucket_name, blob_path = self._split_gcs_path(self.options.manifest_path)
        blob = client.bucket(bucket_name).blob(blob_path)
        content = blob.download_as_text()
        return pd.read_csv(io.StringIO(content))

    def _list_existing_chunks(self, client) -> set[str]:
        """Return the set of chunk names (e.g. ``chunk_00003``) that already have output."""
        output_base = self.options.output_prefix.rstrip("/") + "/"
        bucket_name, prefix = self._split_gcs_path(output_base)
        iterator = client.list_blobs(bucket_name, prefix=prefix, delimiter="/")
        list(iterator)  # consume to populate .prefixes
        return {p[len(prefix):].rstrip("/") for p in (iterator.prefixes or []) if p != prefix}

    @staticmethod
    def _upload_chunk(client, chunk_df: pd.DataFrame, gcs_path: str) -> None:
        """Upload a chunk DataFrame as CSV to GCS."""
        bucket_name, blob_path = GeneticCorrelationManifestGenerator._split_gcs_path(gcs_path)
        blob = client.bucket(bucket_name).blob(blob_path)
        blob.upload_from_string(chunk_df.to_csv(index=False), content_type="text/csv")

    @staticmethod
    def _split_gcs_path(gcs_path: str) -> tuple[str, str]:
        """Split ``gs://bucket/path`` into ``(bucket, path)``."""
        without_scheme = gcs_path[len("gs://"):]
        bucket, _, path = without_scheme.partition("/")
        return bucket, path
