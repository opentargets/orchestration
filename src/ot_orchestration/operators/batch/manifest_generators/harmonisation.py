"""Manifest generator for harmonisation tasks."""

from __future__ import annotations

import logging
import re
from typing import Literal

import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from ot_orchestration.operators.batch.batch_index import BatchIndex
from ot_orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from ot_orchestration.types import ManifestGeneratorSpecs
from ot_orchestration.utils.path import GCSPath

logging.basicConfig(level=logging.DEBUG)


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
        max_task_count: int = 100_000,
    ):
        self.commands = commands
        self.options = options
        self.gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
        self.qc_output_pattern = GCSPath(manifest_kwargs["qc_output_pattern"])
        self.harm_output_pattern = GCSPath(manifest_kwargs["harm_output_pattern"])
        self.raw_input_pattern = GCSPath(manifest_kwargs["raw_input_pattern"])

        self.max_task_count = max_task_count
        self.manifest_path = manifest_kwargs["manifest_output_uri"]

        self.data: dict[
            Literal["raw_sumstat", "harmonised"],
            dict[Literal["sumstat", "study"], list[str]],
        ] = {}
        self.manifest: pd.DataFrame | None = None

    @classmethod
    def from_generator_config(
        cls, specs: ManifestGeneratorSpecs, max_task_count: int
    ) -> ProtoManifestGenerator:
        """Construct generator from config."""
        return cls(
            commands=specs["commands"],
            options=specs["options"],
            manifest_kwargs=specs["manifest_kwargs"],
            max_task_count=max_task_count,
        )

    def generate_batch_index(self) -> BatchIndex:
        """Generate harmonisation manifest."""
        vars_list = (
            self.get_manifest_data()
            .generate_manifest()
            .dump_manifest()
            .convert_manifest_to_vars_list()
        )

        index = BatchIndex(
            vars_list=vars_list,
            options=self.options,
            commands=self.commands,
            max_task_count=self.max_task_count,
        )

        return index

    def get_manifest_data(self) -> HarmonisationManifestGenerator:
        """List raw sumstat and harmonised sumstat paths."""
        globs: dict[Literal["raw_sumstat", "harmonised"], GCSPath] = {
            "raw_sumstat": self.raw_input_pattern,
            "harmonised": self.harm_output_pattern,
        }

        results: dict[
            Literal["raw_sumstat", "harmonised"],
            dict[Literal["sumstat", "study"], list[str]],
        ] = {}
        for key, pattern in globs.items():
            protocol = pattern.segments.get("protocol")
            root = pattern.segments.get("root")
            prefix = pattern.segments.get("prefix")
            match_glob = pattern.segments.get("filename")

            files = self.gcs_hook.list(
                bucket_name=root,
                prefix=prefix,
                match_glob=match_glob,
            )
            logging.info("Found %s %s files", len(files), key)
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

        logging.info("Shape of raw sumstats %s", raw_df.shape)
        logging.info("Shape of harm sumstats %s", harm_df.shape)
        merged_df = raw_df.merge(harm_df, how="left", on="study")
        logging.info("Shape of merged sumstat %s", merged_df.shape)

        # Backfill
        merged_df["isHarmonised"] = merged_df["isHarmonised"].fillna(False)

        expr = lambda x: self.output_path(x, self.qc_output_pattern)
        merged_df["qcPath"] = merged_df["study"].apply(expr)

        expr = lambda x: self.output_path(x, self.harm_output_pattern)
        merged_df["harmonisedSumstatPath"] = merged_df["study"].apply(expr)

        self.manifest = merged_df

        return self

    def dump_manifest(self) -> HarmonisationManifestGenerator:
        """Perform dump of the manifest for downstream processing."""
        if not isinstance(self.manifest, pd.DataFrame):
            raise ValueError("Create manifest first.")
        self.manifest.to_csv(self.manifest_path, index=False)
        return self

    def convert_manifest_to_vars_list(self) -> list[dict[str, str]]:
        """Deconstruct manifest to collect studies to harmonize as a variable list."""
        if not isinstance(self.manifest, pd.DataFrame):
            raise ValueError("Create manifest first.")

        manifest = self.manifest.copy()
        # NOTE: we want to have a var_list with only non harmonised data.
        manifest = manifest[~manifest["isHarmonised"]]
        # Extract only relevant keys
        manifest = manifest[["rawSumstatPath", "harmonisedSumstatPath", "qcPath"]]
        # Rename var_list so we have a clear names
        manifest.rename(columns=self.fields, inplace=True)
        # convert to var_list
        var_list = manifest.to_dict("records")
        if var_list:
            logging.info(var_list[0])
        else:
            logging.warning("No environments to create")
        # NOTE: Ensure the types are correct, as Environment requires only str dictionaries.
        var_list = [{str(k): str(v) for k, v in row.items()} for row in var_list]
        return var_list

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
        pattern = r"\/(GCST\d+)(\.parquet)?\/"
        pattern = re.compile(pattern)
        result = pattern.search(path)
        if not result:
            raise ValueError("Gwas Catalog identifier was not found in %s", path)
        return result.group(1)
