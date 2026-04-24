"""Manifest generator for harmonisation tasks."""

from __future__ import annotations

import logging
import re
from typing import Annotated, Literal

import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pydantic import BaseModel, StringConstraints

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.models.batch.environment import EnvironmentRegistrySpec, EnvironmentSpec
from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators.proto import ProtoManifestGenerator
from orchestration.utils.path import GCSPath

logger = logging.getLogger(__name__)


class HarmonisationManifestGeneratorOptions(BaseModel):
    """Specification for HarmonisationManifestGenerator.

    Example:
    ---
    >>> opts = HarmonisationManifestGeneratorOptions(
    ...     qc_output_pattern="gs://bucket/summary_statistics_qc/**_SUCCESS",
    ...     harm_output_pattern="gs://bucket/harmonised_summary_statistics/**_SUCCESS",
    ...     raw_input_pattern="gs://bucket/raw_summary_statistics/**.h.tsv.gz",
    ...     manifest_output_uri="gs://bucket/harmonisation_manifest.csv",
    ... )
    >>> opts.qc_output_pattern
    'gs://bucket/summary_statistics_qc/**_SUCCESS'
    >>> opts.raw_input_pattern
    'gs://bucket/raw_summary_statistics/**.h.tsv.gz'

    Raw input accepts any dot-separated extension after /**:

    >>> opts2 = HarmonisationManifestGeneratorOptions(
    ...     qc_output_pattern="gs://bucket/summary_statistics_qc/**_SUCCESS",
    ...     harm_output_pattern="gs://bucket/harmonised_summary_statistics/**_SUCCESS",
    ...     raw_input_pattern="gs://bucket/raw_summary_statistics/**.parquet",
    ...     manifest_output_uri="gs://bucket/harmonisation_manifest.csv",
    ... )
    >>> opts2.raw_input_pattern
    'gs://bucket/raw_summary_statistics/**.parquet'

    Output patterns must end with /**_SUCCESS — the old {{study}} placeholder form is rejected:

    >>> from pydantic import ValidationError
    >>> try:
    ...     HarmonisationManifestGeneratorOptions(
    ...         qc_output_pattern="gs://bucket/summary_statistics_qc/{{study}}/",
    ...         harm_output_pattern="gs://bucket/harmonised_summary_statistics/**_SUCCESS",
    ...         raw_input_pattern="gs://bucket/raw_summary_statistics/**.h.tsv.gz",
    ...         manifest_output_uri="gs://bucket/harmonisation_manifest.csv",
    ...     )
    ... except ValidationError:
    ...     print("invalid")
    invalid

    Raw input pattern must include a file extension after /**:

    >>> try:
    ...     HarmonisationManifestGeneratorOptions(
    ...         qc_output_pattern="gs://bucket/summary_statistics_qc/**_SUCCESS",
    ...         harm_output_pattern="gs://bucket/harmonised_summary_statistics/**_SUCCESS",
    ...         raw_input_pattern="gs://bucket/raw_summary_statistics/**",
    ...         manifest_output_uri="gs://bucket/harmonisation_manifest.csv",
    ...     )
    ... except ValidationError:
    ...     print("invalid")
    invalid

    Manifest URI must end with .csv:

    >>> try:
    ...     HarmonisationManifestGeneratorOptions(
    ...         qc_output_pattern="gs://bucket/summary_statistics_qc/**_SUCCESS",
    ...         harm_output_pattern="gs://bucket/harmonised_summary_statistics/**_SUCCESS",
    ...         raw_input_pattern="gs://bucket/raw_summary_statistics/**.h.tsv.gz",
    ...         manifest_output_uri="gs://bucket/harmonisation_manifest.parquet",
    ...     )
    ... except ValidationError:
    ...     print("invalid")
    invalid
    """

    qc_output_pattern: Annotated[
        str, StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)+/\*\*_SUCCESS$")
    ]
    """GCS glob pattern for QC output. Must end with /**_SUCCESS (e.g. gs://bucket/path/**_SUCCESS)."""

    harm_output_pattern: Annotated[
        str, StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)+/\*\*_SUCCESS$")
    ]
    """GCS glob pattern for harmonised output. Must end with /**_SUCCESS (e.g. gs://bucket/path/**_SUCCESS)."""

    raw_input_pattern: Annotated[
        str, StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)+/\*\*(\.[a-zA-Z0-9]+)+$")
    ]
    """GCS glob pattern for raw input files. Must end with /**.<ext> (e.g. gs://bucket/path/**.h.tsv.gz)."""

    manifest_output_uri: Annotated[str, StringConstraints(pattern=r"^gs://[a-zA-Z0-9_-]+(/[a-zA-Z0-9_.-]+)*\.csv$")]
    """GCS path for manifest output. Must end with .csv (e.g. gs://bucket/path/manifest.csv)."""


class HarmonisationManifestGenerator(ProtoManifestGenerator):
    fields = {
        "rawSumstatPath": "RAW",
        "harmonisedSumstatPath": "HARMONISED",
        "qcPath": "QC",
    }

    def __init__(
        self,
        *,
        options: HarmonisationManifestGeneratorOptions,
        gcp_conn_id: str = "google_cloud_default",
    ):
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.qc_output_pattern = GCSPath(options.qc_output_pattern)
        self.harm_output_pattern = GCSPath(options.harm_output_pattern)
        self.raw_input_pattern = GCSPath(options.raw_input_pattern)

        self.manifest_path = options.manifest_output_uri

        self.data: dict[
            Literal["raw_sumstat", "harmonised", "qc"],
            dict[Literal["sumstat", "study"], list[str]],
        ] = {}
        self.manifest: pd.DataFrame | None = None

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> ProtoManifestGenerator:
        """Construct generator from config."""
        return cls(
            options=HarmonisationManifestGeneratorOptions(**specs.generator_options),
        )

    def generate_batch_index(self) -> BatchIndex:
        """Generate harmonisation manifest."""
        env_registry = self._get_manifest_data()._generate_manifest()._dump_manifest()._build_environment_registry()
        return BatchIndex(env_registry=env_registry)

    def _get_manifest_data(self) -> HarmonisationManifestGenerator:
        """List raw sumstat and harmonised sumstat paths."""
        globs: dict[Literal["raw_sumstat", "harmonised", "qc"], GCSPath] = {
            "raw_sumstat": self.raw_input_pattern,
            "harmonised": self.harm_output_pattern,
            "qc": self.qc_output_pattern,
        }

        results: dict[
            Literal["raw_sumstat", "harmonised", "qc"],
            dict[Literal["sumstat", "study"], list[str]],
        ] = {}
        for key, pattern in globs.items():
            protocol = pattern.segments["protocol"]
            root = pattern.segments["root"]
            prefix = pattern.segments["prefix"]
            match_glob = pattern.segments["filename"]

            files = self.gcs_hook.list(
                bucket_name=root,
                # NOTE: ensure the path to the directory is preserved with / so we
                # only list files that are in subdirs of this path, not in
                # any other path that share the last directory name with
                # target path.
                prefix=f"{prefix}/",
                match_glob=match_glob,
            )
            if len(files) == 0 and key == "raw_sumstat":
                logger.warning("No %s files found", key)
                raise AirflowSkipException(f"No {key} files found")
            logger.info("Found %s %s files", len(files), key)
            results[key] = {
                "sumstat": [f"{protocol}://{root}/{s}" for s in files],
                "study": [self._extract_study_id_from_path(s) for s in files],
            }
        self.data = results
        return self

    def _generate_manifest(self) -> HarmonisationManifestGenerator:
        """Construct manifest for sumstat processing.

        This method performs following operations to get the manifest:
        1. fill isHarmonised on harmonised paths
        2. join harmonised and raw paths
        3. backfill isHarmonised when missing harmonised path
        3. construct missing harmonised paths
        4. construct qc paths

        The following method does not take into account the existing qc paths.

        """
        if not self.data:
            self._get_manifest_data()
        raw_df = pd.DataFrame.from_dict(self.data["raw_sumstat"])
        raw_df.rename(columns={"sumstat": "rawSumstatPath"}, inplace=True)
        harm_df = pd.DataFrame.from_dict(self.data["harmonised"])
        harm_df.rename(columns={"sumstat": "harmonisedSumstatPath"}, inplace=True)
        harm_df["isHarmonised"] = True
        qc_df = pd.DataFrame.from_dict(self.data["qc"])
        qc_df.rename(columns={"sumstat": "qcPath"}, inplace=True)
        qc_df["qcPerformed"] = True

        logger.info("Shape of raw sumstats %s", raw_df.shape)
        logger.info("Shape of harm sumstats %s", harm_df.shape)
        logger.info("Shape of qc %s", qc_df.shape)
        # TODO: If single study contains more then 1 summary statistics,
        # Fetch the individual blob datetime and just return the path
        # to latest summary statistics.
        merged_df = raw_df.merge(harm_df, how="left", on="study")
        merged_df2 = merged_df.merge(qc_df, how="left", on="study")
        logger.info("Shape of merged sumstat %s", merged_df2.shape)

        # Backfill
        merged_df2["isHarmonised"] = merged_df2["isHarmonised"].fillna(False)
        merged_df2["qcPerformed"] = merged_df["isHarmonised"].fillna(False)

        expr = lambda x: self._output_path(x, self.qc_output_pattern)
        merged_df2["qcPath"] = merged_df2["study"].apply(expr)

        expr = lambda x: self._output_path(x, self.harm_output_pattern)
        merged_df2["harmonisedSumstatPath"] = merged_df2["study"].apply(expr)

        self.manifest = merged_df2

        return self

    def _dump_manifest(self) -> HarmonisationManifestGenerator:
        """Perform dump of the manifest for downstream processing."""
        if self.manifest is None:
            raise ValueError("Create manifest first.")
        logger.info("Dumping manifest to %s", self.manifest_path)
        self.manifest.to_csv(self.manifest_path, index=False)
        return self

    @staticmethod
    def _validate_manifest_flags(manifest: pd.DataFrame) -> None:
        """Sanity function to ensure that the manifest is correctly prepared for harmonisation."""
        for flag in ["qcPerformed", "isHarmonised"]:
            if flag not in manifest.columns:
                raise ValueError(f"Flag {flag} is missing in manifest")
            values = manifest[flag].drop_duplicates().values
            # Expect the flag to be boolean False only
            assert not values[0] and len(values) == 1, "All non harmonised studies should have qcPerformed set to False"

    def _build_environment_registry(self) -> EnvironmentRegistrySpec:
        """Deconstruct manifest to collect studies to harmonize as a variable list."""
        if self.manifest is None:
            raise ValueError("Create manifest first.")

        manifest = self.manifest.copy()
        # NOTE: we want to have a var_list with only non harmonised data.
        manifest = manifest[~manifest["isHarmonised"]]
        # Skip the execution if there is nothing new to harmonise
        logger.info("Shape of manifest %s", manifest.shape)
        if manifest.empty:
            raise AirflowSkipException("No new studies to harmonise")
        self._validate_manifest_flags(manifest)
        # Extract only relevant keys
        manifest = manifest[["rawSumstatPath", "harmonisedSumstatPath", "qcPath"]]
        # Rename var_list so we have a clear names
        manifest.rename(columns=self.fields, inplace=True)
        var_list = manifest.to_dict("records")
        if var_list:
            logger.info("Variable list is not empty!")
        else:
            raise AirflowSkipException("No environments to create")
        return EnvironmentRegistrySpec(
            environments=[EnvironmentSpec(variables={str(k): str(v) for k, v in row.items()}) for row in var_list]
        )

    @staticmethod
    def _output_path(study: str, path_pattern: GCSPath) -> str:
        """Construct qc output path."""
        bucket = path_pattern.bucket
        protocol = path_pattern.segments["protocol"]
        prefix = path_pattern.segments["prefix"]
        return f"{protocol}://{bucket}/{prefix}/{study}/"

    @staticmethod
    def _extract_study_id_from_path(path: str) -> str:
        """Extract study id from path.

        Args:
            path (str): path to extract study id from.

        Returns:
            str: study id.

        Raises:
            ValueError: when identifier is not found.
        """
        pattern = re.compile(r"\/(GCST\d+)(\.parquet)?\/")
        result = pattern.search(path)
        if not result:
            raise ValueError("Gwas Catalog identifier was not found in %s", path)
        return result.group(1)
