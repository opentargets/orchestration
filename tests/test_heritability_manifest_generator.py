"""Tests for HeritabilityManifestGenerator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowSkipException
from pydantic import ValidationError

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.operators.batch.manifest_generators.heritability_estimate import (
    HeritabilityManifestGenerator,
    HeritabilityManifestGeneratorOptions,
)

BUCKET = "bucket"
BASE = f"gs://{BUCKET}"


def _make_generator() -> tuple[HeritabilityManifestGenerator, MagicMock]:
    opts = HeritabilityManifestGeneratorOptions(
        input_glob=f"{BASE}/input/studies",
        output_prefix=f"{BASE}/output/heritability_estimates",
    )
    with patch("orchestration.operators.batch.manifest_generators.heritability_estimate.GCSHook"):
        gen = HeritabilityManifestGenerator(options=opts)
    mock_hook = MagicMock()
    gen.gcs_hook = mock_hook
    return gen, mock_hook


# Options validation


@pytest.mark.parametrize(
    "input_glob",
    ["gs://valid/bucket/deep/path", "gs://a/b"],
)
def test_valid_input_glob(input_glob: str) -> None:
    opts = HeritabilityManifestGeneratorOptions(input_glob=input_glob, output_prefix=f"{BASE}/out/")
    assert opts.input_glob == input_glob


@pytest.mark.parametrize("output_prefix", ["", f"{BASE}/single/", f"{BASE}/deep/nested/"])
def test_valid_output_prefix(output_prefix: str) -> None:
    opts = HeritabilityManifestGeneratorOptions(input_glob=f"{BASE}/inp", output_prefix=output_prefix)
    assert opts.output_prefix == output_prefix


@pytest.mark.parametrize(
    "input_glob",
    ["s3://bad", "gs://bucket", "", "/local"],
)
def test_invalid_input_glob_raises(input_glob: str) -> None:
    with pytest.raises(ValidationError):
        HeritabilityManifestGeneratorOptions(input_glob=input_glob, output_prefix=f"{BASE}/out/")


@pytest.mark.parametrize("output_prefix", ["s3://bad"])
def test_invalid_output_prefix_raises(output_prefix: str) -> None:
    with pytest.raises(ValidationError):
        HeritabilityManifestGeneratorOptions(input_glob=f"{BASE}/inp", output_prefix=output_prefix)


# from_generator_config


def test_from_generator_config_returns_instance() -> None:
    specs = ManifestGeneratorSpec(
        generator_options={"input_glob": f"{BASE}/input/studies", "output_prefix": f"{BASE}/out/"}
    )
    with patch("orchestration.operators.batch.manifest_generators.heritability_estimate.GCSHook"):
        gen = HeritabilityManifestGenerator.from_generator_config(specs)
    assert isinstance(gen, HeritabilityManifestGenerator)


# build_vars_list


def test_normal_case_returns_partition_vars() -> None:
    gen, hook = _make_generator()
    mock_iter1 = MagicMock()
    mock_iter1.prefixes = ["input/studies/GCST000001/", "input/studies/GCST000002/"]
    mock_iter2 = MagicMock()
    mock_iter2.prefixes = []
    hook.get_conn().list_blobs.side_effect = [mock_iter1, mock_iter2]

    result = gen.build_vars_list()
    assert len(result) == 2
    for row in result:
        assert "INPUT_PARTITION" in row and "OUTPUT_PARTITION" in row


def test_all_existing_outputs_raises_skip() -> None:
    gen, hook = _make_generator()
    mock_iter1 = MagicMock()
    mock_iter1.prefixes = ["input/studies/GCST000001/"]
    mock_iter2 = MagicMock()
    mock_iter2.prefixes = ["output/heritability_estimates/GCST000001/"]
    hook.get_conn().list_blobs.side_effect = [mock_iter1, mock_iter2]

    with pytest.raises(AirflowSkipException):
        gen.build_vars_list()


def test_empty_input_raises_skip() -> None:
    gen, hook = _make_generator()
    empty = MagicMock()
    empty.prefixes = []
    hook.get_conn().list_blobs.side_effect = [empty, empty]

    with pytest.raises(AirflowSkipException):
        gen.build_vars_list()


# generate_batch_index


def test_generate_batch_index() -> None:
    gen, hook = _make_generator()
    mock_iter1 = MagicMock()
    mock_iter1.prefixes = ["input/studies/GCST000001/", "input/studies/GCST000002/"]
    mock_iter2 = MagicMock()
    mock_iter2.prefixes = []
    hook.get_conn().list_blobs.side_effect = [mock_iter1, mock_iter2]

    index = gen.generate_batch_index()
    assert len(index.environment_registry) == 2
    env = index.environment_registry.environments[0]
    assert env.variables["INPUT_PARTITION"] == f"{BASE}/input/studies/GCST000001"
    assert env.variables["OUTPUT_PARTITION"] == f"{BASE}/output/heritability_estimates/GCST000001"
