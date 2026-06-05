"""Batch Collect Operator."""

from __future__ import annotations

import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import cast

from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud.storage import Blob, Bucket

from orchestration.models.batch import BatchCollectSpec


class BatchCollectOperator(BaseOperator):
    """Collects PySpark nested output from GCS into a flat directory.

    After all batch jobs for a step finish, PySpark will have written files
    into per-partition subdirectories under ``collect_spec.source_prefix``
    (e.g. ``credible_set_input_partition_hash=<hash>/part-*.parquet``).
    This operator lists every file matching ``collect_spec.file_glob`` under
    that prefix and copies each one to ``collect_spec.destination_prefix`` with
    a ``part-<index>-<uuid5>-c000.snappy.<ext>`` name, where the UUID5 is derived
    deterministically from the source blob path, so re-running collect for the same
    source files always produces identical destination filenames.

    When ``collect_spec`` is ``None`` the task raises ``AirflowSkipException``
    — this lets the operator be wired unconditionally in the DAG for every
    batch step without requiring steps that don't need collect to opt out.

    Args:
        collect_spec: Collect configuration, or ``None`` for a no-op.
        gcp_conn_id: Airflow GCP connection to use.
        max_workers: Thread-pool size for concurrent GCS copies.
    """

    def __init__(
        self,
        collect_spec: BatchCollectSpec | None,
        gcp_conn_id: str = "google_cloud_default",
        max_workers: int = 20,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.collect_spec = collect_spec
        self.gcp_conn_id = gcp_conn_id
        self.max_workers = max_workers

    def execute(self, context) -> None:
        """Execute the collect operation."""
        if self.collect_spec is None:
            raise AirflowSkipException("No collect spec configured — skipping.")
        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        spec = self.collect_spec
        files = self._list_files(self.collect_spec, hook)
        if not files:
            raise AirflowSkipException(f"No files under {spec.source_prefix!r} matching {spec.file_glob!r} — skipping.")

        self.log.info("Collecting %d file(s) from %s into %s", len(files), spec.source_prefix, spec.destination_prefix)
        client = hook.get_conn()
        src_bucket = client.bucket(spec.source_path.bucket)
        dst_bucket = client.bucket(spec.destination_path.bucket)
        blob_pairs = self._prepare_blob_pairs(
            files, src_bucket, dst_bucket, spec.destination_path.path, spec.file_extension
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            pending = self._submit_copies(executor, blob_pairs)
            for future in as_completed(pending):
                self.log.info("collected %s -> %s", pending[future], future.result())

    @staticmethod
    def _list_files(spec: BatchCollectSpec, hook: GCSHook) -> list[str]:
        return hook.list(
            bucket_name=spec.source_path.bucket,
            prefix=spec.source_path.path,
            match_glob=spec.file_glob,
        )

    @staticmethod
    def _prepare_blob_pairs(
        files: list[str],
        src_bucket: Bucket,
        dst_bucket: Bucket,
        dest_path: str,
        extension: str,
    ) -> list[tuple[Blob, Blob]]:
        return [
            (
                src_bucket.blob(src_name),
                dst_bucket.blob(
                    f"{dest_path}/part-{part_idx:05d}-{uuid.uuid5(uuid.NAMESPACE_URL, src_name)}-c000.snappy.{extension}"
                ),
            )
            for part_idx, src_name in enumerate(sorted(files))
        ]

    @staticmethod
    def _submit_copies(
        executor: ThreadPoolExecutor,
        blob_pairs: list[tuple[Blob, Blob]],
    ) -> dict[Future[str], str]:
        copy = lambda src, dst: cast(str, src.bucket.copy_blob(src, dst.bucket, dst.name).name)
        return {
            executor.submit(copy, src_blob, dst_blob): cast(str, src_blob.name) for src_blob, dst_blob in blob_pairs
        }
