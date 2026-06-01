"""Batch Collect Operator."""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from orchestration.models.batch import BatchCollectSpec


class BatchCollectOperator(BaseOperator):
    """Collects PySpark nested output from GCS into a flat directory.

    After all batch jobs for a step finish, PySpark will have written files
    into per-partition subdirectories under ``collect_spec.source_prefix``
    (e.g. ``credible_set_input_partition_hash=<hash>/part-*.parquet``).
    This operator lists every file matching ``collect_spec.file_glob`` under
    that prefix and copies each one to ``collect_spec.collected_output`` using
    a deterministic ``part-<uuid5>.parquet`` name derived from the full source
    URI, guaranteeing no collisions across partition subdirectories.

    UUID5 seeding makes the operation idempotent: re-running collect for the
    same source always produces identical destination filenames, so reruns do
    not accumulate duplicates.

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

        spec = self.collect_spec
        src_bucket, src_prefix = spec.source_prefix.removeprefix("gs://").split("/", 1)
        dst_bucket, dst_prefix = spec.collected_output.removeprefix("gs://").split("/", 1)
        src_prefix = src_prefix.rstrip("/") + "/"
        dst_prefix = dst_prefix.rstrip("/")

        hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        files = hook.list(
            bucket_name=src_bucket,
            prefix=src_prefix,
            match_glob=spec.file_glob,
        )

        if not files:
            raise AirflowSkipException(
                f"No files under {spec.source_prefix!r} matching {spec.file_glob!r} — skipping."
            )

        self.log.info(
            "Collecting %d file(s) from %s into %s",
            len(files),
            spec.source_prefix,
            spec.collected_output,
        )

        client = hook.get_conn()
        src_bucket_obj = client.bucket(src_bucket)
        dst_bucket_obj = client.bucket(dst_bucket)

        def _copy(source_blob_name: str) -> str:
            full_source_uri = f"gs://{src_bucket}/{source_blob_name}"
            dest_filename = f"part-{uuid.uuid5(uuid.NAMESPACE_URL, full_source_uri).hex}.parquet"
            src_blob = src_bucket_obj.blob(source_blob_name)
            src_bucket_obj.copy_blob(src_blob, dst_bucket_obj, f"{dst_prefix}/{dest_filename}")
            return dest_filename

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(_copy, f): f for f in files}
            for future in as_completed(futures):
                self.log.info("collected %s -> %s", futures[future], future.result())
