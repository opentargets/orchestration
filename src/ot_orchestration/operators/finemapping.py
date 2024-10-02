"""Finemapping operators."""

import time
from functools import cached_property

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from google.cloud.batch import LifecyclePolicy

from ot_orchestration.types import GoogleBatchSpecs
from ot_orchestration.utils.batch import (
    create_batch_job,
    create_task_env,
    create_task_spec,
)
from ot_orchestration.utils.common import GCP_PROJECT_GENETICS, GCP_REGION
from ot_orchestration.utils.path import GCSPath, IOManager, extract_partition_from_blob


class FinemappingBatchJobManifestOperator(BaseOperator):
    """Generate a manifest for a fine-mapping job."""

    def __init__(
        self,
        collected_loci_path: str,
        manifest_prefix: str,
        output_path: str,
        max_records_per_chunk: int = 100_000,
        **kwargs,
    ):
        self.log.info("Using collected loci from %s", collected_loci_path)
        self.log.info("Saving manifests to %s", manifest_prefix)
        self.log.info("The output of the finemapping will be in %s", output_path)
        self.collected_loci_path = GCSPath(collected_loci_path)
        self.manifest_prefix = manifest_prefix
        self.output_path = output_path
        self.max_records_per_chunk = max_records_per_chunk
        super().__init__(**kwargs)

    def execute(self, context):
        """Execute the operator."""
        return self.generate_manifests_for_finemapping()

    @cached_property
    def io_manager(self) -> IOManager:
        """Property to get the IOManager to load and dump files."""
        return IOManager()

    def _extract_study_locus_ids_from_blobs(self, collected_loci_path) -> list[str]:
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
        all_study_locus_ids = [
            extract_partition_from_blob(blob.name)
            for blob in blobs
            if "studyLocusId" in blob.name and blob.name.endswith("/")
        ]
        self.log.info("Found %s studyLocusId(s)", len(all_study_locus_ids))
        return all_study_locus_ids

    def _generate_manifest_rows(self, study_locus_ids: list[str]) -> list[str]:
        """This method generates a list containing all rows that will be used to generate the manifests."""
        self.log.info("Concatenating studyLocusId(s) to create manifest rows.")
        manifest_rows = [
            f"{self.collected_loci_path}/{locus},{self.output_path}/{locus}"
            for locus in study_locus_ids
        ]
        return manifest_rows

    def _partition_rows_by_range(self, manifest_rows: list[str]) -> list[list[str]]:
        """This method partitions rows by pre-defined range."""
        manifest_chunks: list[list[str]] = []
        if self.max_records_per_chunk > len(manifest_rows):
            self.log.warning(
                "Consider down sampling the `max_records_per_chunk` parameter. Currently it outputs 1 partition."
            )
            self.max_records_per_chunk = len(manifest_rows)
        self.log.info(
            "Partitioning %s manifest rows by %s studyLocusId chunks.",
            len(manifest_rows),
            self.max_records_per_chunk,
        )
        for i in range(0, len(manifest_rows), self.max_records_per_chunk):
            chunk = manifest_rows[i : i + self.max_records_per_chunk]
            lines = ["study_locus_input,study_locus_output"] + chunk
            manifest_chunks.append(lines)
            self.log.info("Example output %s", lines[0:2])

        return manifest_chunks

    def _prepare_batch_task_env(
        self, manifest_chunks: list[list[str]]
    ) -> list[tuple[int, str, int]]:
        """Get the environment that will be used by batch tasks."""
        transfer_objects = []
        env_objects: list[tuple[int, str, int]] = []
        for i, lines in enumerate(manifest_chunks):
            self.log.info("Amending %s lines for %s manifest", len(lines) - 1, i)
            text = "\n".join(lines)
            manifest_path = f"{self.manifest_prefix}/chunk_{i}"
            self.log.info("Writing manifest to %s.", manifest_path)
            transfer_objects.append((manifest_path, text))
            env_objects.append((i, manifest_path, len(lines) - 1))

        self.log.info("Writing %s manifests", len(transfer_objects))
        self.io_manager.dump_many(
            paths=[t[0] for t in transfer_objects],
            objects=[t[1] for t in transfer_objects],
        )
        return env_objects

    def generate_manifests_for_finemapping(self) -> list[tuple[int, str, int]]:
        """Starting from collected_loci, generate manifests for finemapping, splitting in chunks of at most 100,000 records.

        This step saves the manifests to GCS under the manifest_prefix path with suffix `chunk_{i}`. Each chunk is a csv
        file with two columns: study_locus_input and study_locus_output.

        Return:
            list[(int, str, int)]: List of tuples, where the first value is index of the manifest, the second value is a path to manifest, and the third is the number of records in that manifest.
        """
        all_study_locus_ids = self._extract_study_locus_ids_from_blobs(
            self.collected_loci_path
        )
        manifest_rows = self._generate_manifest_rows(all_study_locus_ids)
        manifest_chunks = self._partition_rows_by_range(manifest_rows)
        environments = self._prepare_batch_task_env(manifest_chunks)
        return environments


class FinemappingBatchOperator(CloudBatchSubmitJobOperator):
    def __init__(
        self,
        manifest: tuple[int, str, int],
        study_index_path: str,
        google_batch: GoogleBatchSpecs,
        **kwargs,
    ):
        self.study_index_path = study_index_path
        self.idx, self.study_locus_manifest_path, self.num_of_tasks = manifest

        super().__init__(
            project_id=GCP_PROJECT_GENETICS,
            region=GCP_REGION,
            job_name=f"finemapping-job-{self.idx}-{time.strftime('%Y%m%d-%H%M%S')}",
            job=create_batch_job(
                task=create_task_spec(
                    image=google_batch["image"],
                    commands=self.susie_finemapping_command,
                    task_specs=google_batch["task_specs"],
                    resource_specs=google_batch["resource_specs"],
                    entrypoint=google_batch["entrypoint"],
                    lifecycle_policies=[
                        LifecyclePolicy(
                            action=LifecyclePolicy.Action.FAIL_TASK,
                            action_condition=LifecyclePolicy.ActionCondition(
                                exit_codes=[50005]  # Execution time exceeded.
                            ),
                        )
                    ],
                ),
                task_env=create_task_env(
                    var_list=[
                        {"LOCUS_INDEX": str(idx)} for idx in range(0, manifest[2])
                    ]
                ),
                policy_specs=google_batch["policy_specs"],
            ),
            deferrable=False,
            **kwargs,
        )

    @property
    def susie_finemapping_command(self) -> list[str]:
        """Get the command for running the finemapping batch job."""
        return [
            "-c",
            (
                "poetry run gentropy "
                "step=susie_finemapping "
                f"step.study_index_path={self.study_index_path} "
                f"step.study_locus_manifest_path={self.study_locus_manifest_path} "
                "step.study_locus_index=$LOCUS_INDEX "
                "step.max_causal_snps=10 "
                "step.lead_pval_threshold=1e-5 "
                "step.purity_mean_r2_threshold=0 "
                "step.purity_min_r2_threshold=0.25 "
                "step.cs_lbf_thr=2 step.sum_pips=0.99 "
                "step.susie_est_tausq=False "
                "step.run_carma=False "
                "step.run_sumstat_imputation=False "
                "step.carma_time_limit=600 "
                "step.imputed_r2_threshold=0.9 "
                "step.ld_score_threshold=5 "
                "step.carma_tau=0.15 "
                "step.ld_min_r2=0.8 "
                "+step.session.extended_spark_conf={spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar} "
                "+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:false} "
                "+step.session.extended_spark_conf={spark.driver.memory:30g} "
                "+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:500m} "
                "+step.session.extended_spark_conf={spark.driver.maxResultSize:5g} "
                "step.session.write_mode=overwrite"
            ),
        ]
