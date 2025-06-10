"""Manifest generator for harmonisation tasks."""

from __future__ import annotations

import logging
import re
from typing import Literal

import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from orchestration.types import ManifestGeneratorSpecs
from orchestration.utils.path import GCSPath

logger = logging.getLogger(__name__)


class HarmonisationManifestGenerator(ProtoManifestGenerator):
    # Values of the fields that should be referred to when creating command.
    fields = {
        "harmonisedSumstatPath": "HARMONISED",
        "qcPath": "QC",
        "rawSumstatPath": "RAW",
    }

    @staticmethod
    def safe_join_paths(right: str, left: str) -> str:
        """Safely join paths."""
        right = right.removesuffix("/")
        left = left.removeprefix("/")

        return right + "/" + left

    def __init__(
        self,
        *,
        commands: list[str],
        options: dict[str, str],
        manifest_kwargs: dict[str, str],
        gcp_conn_id: str = "google_cloud_default",
    ):
        self.commands = commands
        self.options = options
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.qc_output_pattern = GCSPath(manifest_kwargs["qc_output_pattern"])
        self.harm_output_pattern = GCSPath(manifest_kwargs["harm_output_pattern"])
        self.raw_input_pattern = GCSPath(manifest_kwargs["raw_input_pattern"])

        self.manifest_path = manifest_kwargs["manifest_output_uri"]

        self.data: dict[
            Literal["raw_sumstat", "harmonised", "qc"],
            dict[Literal["sumstat", "study"], list[str]],
        ] = {}
        self.manifest: pd.DataFrame | None = None

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpecs) -> ProtoManifestGenerator:
        """Construct generator from config."""
        return cls(
            commands=specs["commands"],
            options=specs["options"],
            manifest_kwargs=specs["manifest_kwargs"],
        )

    def generate_batch_index(self) -> BatchIndex:
        """Generate harmonisation manifest."""
        vars_list = self.get_manifest_data().generate_manifest().dump_manifest().convert_manifest_to_vars_list()

        return BatchIndex(
            vars_list=vars_list,
            options=self.options,
            commands=self.commands,
        )

    def get_manifest_data(self) -> HarmonisationManifestGenerator:
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
                "study": [self.extract_study_id_from_path(s) for s in files],
            }
        self.data = results
        return self

    def generate_manifest(self) -> HarmonisationManifestGenerator:
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
            self.get_manifest_data()
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
        merged_df = raw_df.merge(harm_df, how="left", on="study")
        merged_df2 = merged_df.merge(qc_df, how="left", on="study")
        logger.info("Shape of merged sumstat %s", merged_df2.shape)

        # Backfill
        merged_df2["isHarmonised"] = merged_df2["isHarmonised"].fillna(False)
        merged_df2["qcPerformed"] = merged_df["isHarmonised"].fillna(False)

        expr = lambda x: self.output_path(x, self.qc_output_pattern)
        merged_df2["qcPath"] = merged_df2["study"].apply(expr)

        expr = lambda x: self.output_path(x, self.harm_output_pattern)
        merged_df2["harmonisedSumstatPath"] = merged_df2["study"].apply(expr)

        self.manifest = merged_df2

        return self

    def dump_manifest(self) -> HarmonisationManifestGenerator:
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

    def convert_manifest_to_vars_list(self) -> list[dict[str, str]]:
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
        # convert to list of dictionaries
        var_list = manifest.to_dict("records")
        if var_list:
            logger.info("Variable list is not empty!")
        else:
            AirflowSkipException("No environments to create")
        # NOTE: Ensure the types are correct, as Environment requires dict[str,str] types.
        return [{str(k): str(v) for k, v in row.items()} for row in var_list]

    @staticmethod
    def output_path(study: str, path_pattern: GCSPath) -> str:
        """Construct qc output path."""
        bucket = path_pattern.bucket
        protocol = path_pattern.segments["protocol"]
        prefix = path_pattern.segments["prefix"]
        return f"{protocol}://{bucket}/{prefix}/{study}/"

    @staticmethod
    def extract_study_id_from_path(path: str) -> str:
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
