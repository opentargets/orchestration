"""Manifest generator for SuSiE fine-mapping batch tasks."""

from __future__ import annotations

import logging

from pydantic import BaseModel

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.models.batch.environment import EnvironmentRegistrySpec, EnvironmentSpec
from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.operators.batch.manifest_generators.proto import ProtoManifestGenerator
from orchestration.utils.path import GCSPath, extract_partition_from_blob


class FinemappingManifestOptions(BaseModel):
    """Options for the FinemappingManifestGenerator."""

    collected_loci_path: str
    """GCS path to the collected loci. This should be a path to the directory that contains the collected loci partitioned by studyLocusId."""
    manifest_output_path: str
    """GCS path prefix where the generated manifests will be stored. The final manifest paths will have suffix `chunk_{i}`."""
    output_path: str
    """GCS path prefix where the output of the finemapping will be stored. The final output paths will have suffix `{studyLocusId}`."""
    log_path: str
    """GCS path prefix where the logs of the finemapping will be stored. The final log paths will have suffix `{studyLocusId}`."""


class FinemappingManifestGenerator(ProtoManifestGenerator):
    """Generate a manifest for a fine-mapping job."""

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> FinemappingManifestGenerator:
        """Construct generator from config."""
        return cls(options=FinemappingManifestOptions(**specs.generator_options))

    def __init__(
        self,
        options: FinemappingManifestOptions,
        **kwargs,
    ):
        self.log = logging.getLogger(__name__)

        self.log.info("Using collected loci from %s", options.collected_loci_path)
        self.log.info("Saving manifest to %s", options.manifest_output_path)
        self.log.info("The output of the finemapping will be in %s", options.output_path)
        self.log.info("The logs of the finemapping will be in %s", options.log_path)
        self.collected_loci_path = GCSPath(options.collected_loci_path)
        self.manifest_output_path = GCSPath(options.manifest_output_path)
        self.output_path = GCSPath(options.output_path)
        self.log_path = GCSPath(options.log_path)

        super().__init__(**kwargs)

    def generate_batch_index(self) -> BatchIndex:
        return BatchIndex(env_registry=self._build_environment_registry())

    def _build_environment_registry(self) -> EnvironmentRegistrySpec:
        """Build environment registry for the batch tasks."""
        all_study_locus_ids = self._extract_study_locus_ids_from_blobs()
        finemapped_study_locus_ids = self._extract_loci_from_logfiles()
        study_locus_ids = list(all_study_locus_ids - finemapped_study_locus_ids)
        manifest_rows = self._generate_manifest_rows(study_locus_ids)
        self._save_manifest(manifest_rows)
        return EnvironmentRegistrySpec(
            environments=[
                EnvironmentSpec(variables={"LOCUS_INDEX": str(locus_index)})
                for locus_index in range(len(manifest_rows))
            ]
        )

    def _generate_manifest_rows(self, study_locus_ids: list[str]) -> list[str]:
        """This method generates a list containing all rows that will be used to generate the manifests."""
        self.log.info("Concatenating studyLocusId(s) to create manifest rows.")
        manifest_rows: list[str] = []
        for locus in study_locus_ids:
            input_loci_path = f"{self.collected_loci_path}/studyLocusId={locus}"
            # NOTE: make sure that outputs do not preserve the partitions inside output paths derived from the input loci paths.
            output_loci_path = f"{self.output_path}/{locus}"
            log_path = f"{self.log_path}/{locus}"
            manifest_row = ",".join([input_loci_path, output_loci_path, log_path])
            manifest_rows.append(manifest_row)
        manifest_rows.sort()  # ensure that the order of the rows is deterministic.
        return manifest_rows

    def _extract_study_locus_ids_from_blobs(self) -> set[str]:
        """Get list of loci from the input Google Storage path.

        NOTE: This step requires the dataset to be partitioned only by StudyLocusId!!
        """
        self.log.info(
            "Extracting studyLocusId from partition names in %s.",
            self.collected_loci_path,
        )
        client = self.collected_loci_path.client
        bucket = client.get_bucket(self.collected_loci_path.bucket)
        blobs = bucket.list_blobs(prefix=self.collected_loci_path.path)
        # Use set to avoid duplicates that comes from the
        # multiple parquet files and directory.
        all_study_locus_ids = {
            # ensure that we do not retain the schema of the
            extract_partition_from_blob(blob.name, with_prefix=False)
            for blob in blobs
            if "studyLocusId" in blob.name
        }
        self.log.info("Found %s studyLocusId(s)", len(all_study_locus_ids))
        return all_study_locus_ids

    def _extract_loci_from_logfiles(self) -> set[str]:
        """Get list of loci from the output Google Storage path."""
        self.log.info("Extracting studyLocusId from partition names in %s.", self.log_path)
        client = self.log_path.client
        bucket = client.get_bucket(self.log_path.bucket)
        blobs = bucket.list_blobs(prefix=self.log_path.path)
        self.log.info("prefix: %s", self.log_path.path)

        # NOTE: these blobs are not partitioned, so we need to retain only the StudyLocusId.
        # The blobs should be following this convention `credible_set_datasets/${studyLocusId}/_SUCCESS`
        all_study_locus_ids = {
            blob.name.removeprefix(self.log_path.path).removesuffix(".log").replace("/", "")
            for blob in blobs
            if blob.name.endswith(".log")
        }
        self.log.info("Found %s studyLocusId(s) that were finemapped.", len(all_study_locus_ids))
        return all_study_locus_ids

    def _save_manifest(self, manifest_rows: list[str]) -> None:
        """Save the manifest to GCS."""
        header = "study_locus_input,study_locus_output,log_output"
        self.log.info("Amending %s lines for %s manifest", len(manifest_rows), self.manifest_output_path)

        manifest = "\n".join([header, *manifest_rows])
        self.manifest_output_path.dump(manifest)
