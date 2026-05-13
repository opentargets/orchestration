"""Tests for HarmonisationManifestGenerator."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from airflow.exceptions import AirflowSkipException

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.models.batch.environment import EnvironmentRegistrySpec, EnvironmentSpec
from orchestration.operators.batch.manifest_generators.harmonisation import (
    HarmonisationManifestGenerator,
    HarmonisationManifestGeneratorOptions,
)

BUCKET = "bucket"
BASE = f"gs://{BUCKET}"

RAW_BLOBS = [
    "raw/GCST000001.parquet/part.parquet",
    "raw/GCST000002.parquet/part.parquet",
    "raw/GCST000003.parquet/part.parquet",
]
HARMONISED_BLOBS = [
    "harmonised/GCST000001/_SUCCESS",
    "harmonised/GCST000002/_SUCCESS",
]
QC_BLOBS = [
    "qc/GCST000001/_SUCCESS",
    "qc/GCST000002/_SUCCESS",
]


@pytest.fixture
def options() -> HarmonisationManifestGeneratorOptions:
    return HarmonisationManifestGeneratorOptions(
        qc_output_pattern=f"{BASE}/qc/**_SUCCESS",
        harm_output_pattern=f"{BASE}/harmonised/**_SUCCESS",
        raw_input_pattern=f"{BASE}/raw/**.parquet",
        manifest_output_uri=f"{BASE}/manifest.csv",
    )


@pytest.fixture
def generator(options: HarmonisationManifestGeneratorOptions) -> HarmonisationManifestGenerator:
    with patch("orchestration.operators.batch.manifest_generators.harmonisation.GCSHook") as mock_cls:
        mock_hook = MagicMock()
        mock_cls.return_value = mock_hook
        gen = HarmonisationManifestGenerator(options=options)
    gen.gcs_hook = mock_hook
    return gen


def _configure_list(
    generator: HarmonisationManifestGenerator,
    raw: list[str] = RAW_BLOBS,
    harmonised: list[str] = HARMONISED_BLOBS,
    qc: list[str] = QC_BLOBS,
) -> None:
    """Set GCSHook.list side_effect for one _get_manifest_data() call."""
    cast(MagicMock, generator.gcs_hook).list.side_effect = [raw, harmonised, qc]


# ---------------------------------------------------------------------------
# from_generator_config
# ---------------------------------------------------------------------------


class TestHarmonisationManifestGenerator:
    @pytest.mark.parametrize(
        "generator_options",
        [
            pytest.param(
                {
                    "qc_output_pattern": f"{BASE}/qc/**_SUCCESS",
                    "harm_output_pattern": f"{BASE}/harmonised/**_SUCCESS",
                    "raw_input_pattern": f"{BASE}/raw/**.parquet",
                    "manifest_output_uri": f"{BASE}/manifest.csv",
                },
                id="standard",
            ),
            pytest.param(
                {
                    "qc_output_pattern": "gs://other-bucket/summary_statistics_qc/**_SUCCESS",
                    "harm_output_pattern": "gs://other-bucket/harmonised_summary_statistics/**_SUCCESS",
                    "raw_input_pattern": "gs://other-bucket/raw/**.h.tsv.gz",
                    "manifest_output_uri": "gs://other-bucket/manifest.csv",
                },
                id="different_bucket_and_extensions",
            ),
        ],
    )
    def test__constructor_returns_generator_instance(self, generator_options: dict[str, str]) -> None:
        specs = ManifestGeneratorSpec(generator_options=generator_options)
        with patch("orchestration.operators.batch.manifest_generators.harmonisation.GCSHook"):
            gen = HarmonisationManifestGenerator.from_generator_config(specs)
        assert isinstance(gen, HarmonisationManifestGenerator)

    @pytest.mark.parametrize(
        ("raw", "harmonised", "qc", "expected_raw", "expected_harm", "expected_qc"),
        [
            pytest.param(RAW_BLOBS, HARMONISED_BLOBS, QC_BLOBS, 3, 2, 2, id="normal"),
            pytest.param(RAW_BLOBS[:1], [], [], 1, 0, 0, id="only_raw"),
        ],
    )
    def test__get_manifest_data(
        self,
        generator: HarmonisationManifestGenerator,
        raw: list[str],
        harmonised: list[str],
        qc: list[str],
        expected_raw: int,
        expected_harm: int,
        expected_qc: int,
    ) -> None:
        _configure_list(generator, raw=raw, harmonised=harmonised, qc=qc)
        generator._get_manifest_data()
        assert len(generator.data["raw_sumstat"]["study"]) == expected_raw
        assert len(generator.data["harmonised"]["study"]) == expected_harm
        assert len(generator.data["qc"]["study"]) == expected_qc
        assert generator.gcs_hook.list.call_count == 3  # type: ignore

    def test__get_manifest_data_empty_raw_raises_airflow_skip(self, generator: HarmonisationManifestGenerator) -> None:
        _configure_list(generator, raw=[])
        with pytest.raises(AirflowSkipException):
            generator._get_manifest_data()

    @pytest.mark.parametrize(
        "expected_prefix",
        [pytest.param("raw/", id="raw"), pytest.param("harmonised/", id="harmonised"), pytest.param("qc/", id="qc")],
    )
    def test__get_manifest_data_gcs_hook_prefixes(
        self, generator: HarmonisationManifestGenerator, expected_prefix: str
    ) -> None:
        _configure_list(generator)
        generator._get_manifest_data()
        prefixes = {c.kwargs["prefix"] for c in cast(MagicMock, generator.gcs_hook).list.call_args_list}
        assert expected_prefix in prefixes

    @pytest.mark.parametrize(
        ("harmonised_blobs", "expected_harmonised", "expected_unharmonised"),
        [
            pytest.param(HARMONISED_BLOBS, 2, 1, id="two_harmonised"),
            pytest.param([], 0, 3, id="none_harmonised"),
            pytest.param(
                [*HARMONISED_BLOBS, "harmonised/GCST000003/_SUCCESS"],
                3,
                0,
                id="all_harmonised",
            ),
        ],
    )
    def test__get_manifest_data_harmonised_counts(
        self,
        generator: HarmonisationManifestGenerator,
        harmonised_blobs: list[str],
        expected_harmonised: int,
        expected_unharmonised: int,
    ) -> None:
        _configure_list(generator, harmonised=harmonised_blobs)
        generator._get_manifest_data()._generate_manifest()
        assert isinstance(generator.manifest, pd.DataFrame)
        assert generator.manifest["isHarmonised"].sum() == expected_harmonised
        assert (~generator.manifest["isHarmonised"]).sum() == expected_unharmonised

    def test_all_raw_studies_present(self, generator: HarmonisationManifestGenerator) -> None:
        _configure_list(generator)
        generator._get_manifest_data()._generate_manifest()
        assert isinstance(generator.manifest, pd.DataFrame)
        assert len(generator.manifest) == len(RAW_BLOBS)

    @pytest.mark.parametrize(
        ("study", "expected_harm_path", "expected_qc_path"),
        [
            pytest.param("GCST000003", f"{BASE}/harmonised/GCST000003/", f"{BASE}/qc/GCST000003/", id="unharmonised"),
            pytest.param("GCST000001", f"{BASE}/harmonised/GCST000001/", f"{BASE}/qc/GCST000001/", id="harmonised"),
        ],
    )
    def test__get_manifest_data_output_paths_for_study(
        self,
        generator: HarmonisationManifestGenerator,
        study: str,
        expected_harm_path: str,
        expected_qc_path: str,
    ) -> None:
        _configure_list(generator)
        generator._get_manifest_data()._generate_manifest()
        assert isinstance(generator.manifest, pd.DataFrame)
        row = generator.manifest[generator.manifest["study"] == study].iloc[0]
        assert row["harmonisedSumstatPath"] == expected_harm_path
        assert row["qcPath"] == expected_qc_path

    @pytest.mark.parametrize(
        "manifest_uri",
        [
            pytest.param(f"{BASE}/manifest.csv", id="standard_path"),
        ],
    )
    def test__dump_manifest_calls_to_csv_with_manifest_uri(
        self, generator: HarmonisationManifestGenerator, manifest_uri: str
    ) -> None:
        generator.manifest = MagicMock(spec=pd.DataFrame)
        generator.manifest_path = manifest_uri
        generator._dump_manifest()
        generator.manifest.to_csv.assert_called_once_with(manifest_uri, index=False)

    def test__dump_manifest_raises_if_manifest_is_none(self, generator: HarmonisationManifestGenerator) -> None:
        with pytest.raises(ValueError, match="Create manifest first"):
            generator._dump_manifest()

    @pytest.mark.parametrize(
        ("harmonised_blobs", "expected_env_count"),
        [
            pytest.param(HARMONISED_BLOBS, 1, id="one_unharmonised"),
            pytest.param([], 3, id="all_unharmonised"),
        ],
    )
    def test__build_environment_registry_env_count(
        self,
        generator: HarmonisationManifestGenerator,
        harmonised_blobs: list[str],
        expected_env_count: int,
    ) -> None:
        _configure_list(generator, harmonised=harmonised_blobs)
        generator._get_manifest_data()._generate_manifest()
        registry = generator._build_environment_registry()
        assert len(registry.environments) == expected_env_count

    def test_env_variables_contain_raw_harmonised_qc_keys(self, generator: HarmonisationManifestGenerator) -> None:
        _configure_list(generator)
        generator._get_manifest_data()._generate_manifest()
        registry = generator._build_environment_registry()
        assert all(set(e.variables.keys()) == {"RAW", "HARMONISED", "QC"} for e in registry.environments)

    def test__build_environment_registry_all_harmonised_raises_skip(
        self, generator: HarmonisationManifestGenerator
    ) -> None:
        _configure_list(generator, harmonised=[*HARMONISED_BLOBS, "harmonised/GCST000003/_SUCCESS"])
        generator._get_manifest_data()._generate_manifest()
        with pytest.raises(AirflowSkipException):
            generator._build_environment_registry()

    def test__build_environment_registry_returns_environment_registry_spec(
        self, generator: HarmonisationManifestGenerator
    ) -> None:
        _configure_list(generator)
        generator._get_manifest_data()._generate_manifest()
        registry = generator._build_environment_registry()
        assert isinstance(registry, EnvironmentRegistrySpec)
        assert all(isinstance(e, EnvironmentSpec) for e in registry.environments)

    def test__validate_manifest_flags_passes_for_all_false_flags(self) -> None:
        df = pd.DataFrame({"qcPerformed": [False, False], "isHarmonised": [False, False]})
        HarmonisationManifestGenerator._validate_manifest_flags(df)

    @pytest.mark.parametrize(
        ("df", "expected_exc", "match"),
        [
            pytest.param(
                pd.DataFrame({"isHarmonised": [False]}),
                ValueError,
                "qcPerformed",
                id="missing_qc_performed",
            ),
            pytest.param(
                pd.DataFrame({"qcPerformed": [False]}),
                ValueError,
                "isHarmonised",
                id="missing_is_harmonised",
            ),
            pytest.param(
                pd.DataFrame({"qcPerformed": [False, True], "isHarmonised": [False, True]}),
                AssertionError,
                None,
                id="true_values_present",
            ),
        ],
    )
    def test__validate_manifest_flags_invalid_manifest_raises(
        self, df: pd.DataFrame, expected_exc: type[Exception], match: str | None
    ) -> None:
        kwargs = {"match": match} if match else {}
        with pytest.raises(expected_exc, **kwargs):
            HarmonisationManifestGenerator._validate_manifest_flags(df)

    @pytest.mark.parametrize(
        ("study", "pattern_uri", "expected"),
        [
            pytest.param(
                "GCST000001",
                f"{BASE}/harmonised/**_SUCCESS",
                f"{BASE}/harmonised/GCST000001/",
                id="harmonised",
            ),
            pytest.param(
                "GCST999999",
                f"{BASE}/qc/**_SUCCESS",
                f"{BASE}/qc/GCST999999/",
                id="qc",
            ),
            pytest.param(
                "GCST000001",
                "gs://other-bucket/deep/nested/path/**_SUCCESS",
                "gs://other-bucket/deep/nested/path/GCST000001/",
                id="nested_prefix",
            ),
        ],
    )
    def test__output_path(self, study: str, pattern_uri: str, expected: str) -> None:
        from orchestration.utils.path import GCSPath

        result = HarmonisationManifestGenerator._output_path(study, GCSPath(pattern_uri))
        assert result == expected

    @pytest.mark.parametrize(
        "study", [pytest.param("GCST000001", id="gcst"), pytest.param("GCST999999", id="gcst_long")]
    )
    def test__output_path_always_has_trailing_slash(self, study: str) -> None:
        from orchestration.utils.path import GCSPath

        result = HarmonisationManifestGenerator._output_path(study, GCSPath(f"{BASE}/qc/**_SUCCESS"))
        assert result.endswith("/")

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            pytest.param("raw/GCST123456.parquet/part.parquet", "GCST123456", id="parquet_partition"),
            pytest.param("harmonised/GCST000001/_SUCCESS", "GCST000001", id="success_marker"),
            pytest.param("qc/GCST999999/_SUCCESS", "GCST999999", id="qc_success"),
            pytest.param("deep/nested/GCST111111.parquet/data.parquet", "GCST111111", id="nested_path"),
        ],
    )
    def test__extract_study_id_from_path(self, path: str, expected: str) -> None:
        assert HarmonisationManifestGenerator._extract_study_id_from_path(path) == expected

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("no_gcst_id/file.parquet", id="no_id"),
            pytest.param("raw/not_a_study/data.parquet", id="wrong_prefix"),
        ],
    )
    def test__extract_study_id_from_path_raises_for_unrecognised_path(self, path: str) -> None:
        with pytest.raises(ValueError):
            HarmonisationManifestGenerator._extract_study_id_from_path(path)

    @pytest.mark.parametrize(
        ("harmonised_blobs", "expected_rows"),
        [
            pytest.param(HARMONISED_BLOBS, 1, id="one_unharmonised"),
            pytest.param([], 3, id="all_unharmonised"),
        ],
    )
    def test__batch_index_env_count(
        self,
        generator: HarmonisationManifestGenerator,
        harmonised_blobs: list[str],
        expected_rows: int,
    ) -> None:
        _configure_list(generator, harmonised=harmonised_blobs)
        with patch.object(generator, "_dump_manifest", return_value=generator):
            batch_index = generator.generate_batch_index()
        assert len(batch_index.environment_registry) == expected_rows

    def test__generate_batch_index_calls_dump_manifest(self, generator: HarmonisationManifestGenerator) -> None:
        _configure_list(generator)
        with patch.object(generator, "_dump_manifest", return_value=generator) as mock_dump:
            generator.generate_batch_index()
        mock_dump.assert_called_once()
