"""Tests for BatchCollectOperator static methods."""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, call

import pytest

from orchestration.models.batch.operator import BatchCollectSpec
from orchestration.operators.batch.batch_collect_operator import BatchCollectOperator


@pytest.fixture
def spec() -> BatchCollectSpec:
    return BatchCollectSpec(
        source_prefix="gs://my-bucket/source/path",
        destination_prefix="gs://my-bucket/dest/path",
    )


def test_list_files_passes_correct_args_to_hook(spec: BatchCollectSpec) -> None:
    hook = MagicMock()
    hook.list.return_value = []

    BatchCollectOperator._list_files(spec, hook)

    hook.list.assert_called_once_with(
        bucket_name="my-bucket",
        prefix="source/path",
        match_glob="**.parquet",
    )


def test_prepare_blob_pairs_sorts_files_and_builds_correct_names() -> None:
    src_bucket = MagicMock()
    dst_bucket = MagicMock()
    BatchCollectOperator._prepare_blob_pairs(
        ["b.parquet", "a.parquet"], src_bucket, dst_bucket, "dest/path", "parquet"
    )

    assert src_bucket.blob.call_args_list == [call("a.parquet"), call("b.parquet")]
    assert dst_bucket.blob.call_args_list == [
        call(f"dest/path/part-00000-{uuid.uuid5(uuid.NAMESPACE_URL, 'a.parquet')}-c000.snappy.parquet"),
        call(f"dest/path/part-00001-{uuid.uuid5(uuid.NAMESPACE_URL, 'b.parquet')}-c000.snappy.parquet"),
    ]


def test_submit_copies_maps_futures_to_source_blob_names() -> None:
    src_a, src_b = MagicMock(), MagicMock()
    src_a.name, src_b.name = "a.parquet", "b.parquet"
    blob_pairs = [(src_a, MagicMock()), (src_b, MagicMock())]

    with ThreadPoolExecutor(max_workers=2) as executor:
        pending = BatchCollectOperator._submit_copies(executor, blob_pairs)  # type: ignore

    assert set(pending.values()) == {"a.parquet", "b.parquet"}
