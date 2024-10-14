"""VEP annotation operator."""

from __future__ import annotations

import time
from collections import OrderedDict
from functools import cached_property
from pathlib import Path
from typing import Set, Type

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from google.cloud.storage import Blob, Client

from ot_orchestration.types import GCSMountObject, GoogleBatchSpecs
from ot_orchestration.utils.batch import (
    create_batch_job,
    create_task_env,
    create_task_spec,
)
from ot_orchestration.utils.common import GCP_PROJECT_GENETICS, GCP_REGION
from ot_orchestration.utils.path import GCSPath


class VepAnnotationPathManager:
    """It is quite complicated to keep track of all the input/output buckets, the corresponding mounting points prefixes etc..."""

    def __init__(
        self,
        vcf_input_path: str,
        vep_output_path: str,
        vep_cache_path: str,
        mount_dir_root: str,
    ):
        self.paths = {
            "input": GCSPath(vcf_input_path),
            "output": GCSPath(vep_output_path),
            "cache": GCSPath(vep_cache_path),
        }
        self.mount_dir_root = mount_dir_root

        self._validate_mount_dir()
        self.path_registry = self._prepare_path_registry()
        # explicitly set the properties
        self.cache_dir = self.path_registry["cache"]["mount_point"]
        self.input_dir = self.path_registry["input"]["mount_point"]
        self.output_dir = self.path_registry["output"]["mount_point"]

    def _validate_mount_dir(self) -> VepAnnotationPathManager:
        if not self.mount_dir_root.startswith("/"):
            raise ValueError("Mount dir has to be an absolute path.")
        return self

    def _prepare_path_registry(self) -> dict[str, GCSMountObject]:
        return {
            key: {
                # NOTE: remote_path has to start from the bucket_name
                # see https://cloud.google.com/batch/docs/create-run-job-storage#gcloud_2:~:text=BUCKET_PATH%3A%20the%20path,the%20subdirectory%20subdirectory.
                "remote_path": f"{value.bucket}/{value.path}",
                "mount_point": f"{self.mount_dir_root}/{key}",
            }
            for key, value in self.paths.items()
        }

    def get_mount_config(self) -> list[GCSMountObject]:
        """Return the mount configuration.

        Returns:
            list[dict[str, str]]: The mount configuration.
        """
        return list(self.path_registry.values())


class VepAnnotateOperator(CloudBatchSubmitJobOperator):
    def __init__(
        self,
        task_id: str,
        vcf_input_path: str,
        vep_output_path: str,
        vep_cache_path: str,
        google_batch: GoogleBatchSpecs,
        mount_dir_root: str = "/mnt/disks/share",
        project_id: str = GCP_PROJECT_GENETICS,
        **kwargs,
    ):
        self.project_id = project_id
        self.pm = VepAnnotationPathManager(
            vcf_input_path=vcf_input_path,
            vep_output_path=vep_output_path,
            vep_cache_path=vep_cache_path,
            mount_dir_root=mount_dir_root,
        )
        self.vcf_files = self._get_vcf_partition_basenames()
        environments = [
            {"INPUT_FILE": file, "OUTPUT_FILE": file} for file in self.vcf_files
        ]

        batch_job = create_batch_job(
            task=create_task_spec(
                image=google_batch["image"],
                commands=self._vep_command,
                resource_specs=google_batch["resource_specs"],
                task_specs=google_batch["task_specs"],
                entrypoint=google_batch["entrypoint"],
            ),
            task_env=create_task_env(environments),
            policy_specs=google_batch["policy_specs"],
            mounting_points=self.pm.get_mount_config(),
        )
        self.log.info(batch_job)
        super().__init__(
            task_id=task_id,
            project_id=self.project_id,
            region=GCP_REGION,
            job_name=f"vep-job-{time.strftime('%Y%m%d-%H%M%S')}",
            job=batch_job,
            deferrable=False,
            **kwargs,
        )

    def _get_vcf_partition_basenames(self) -> Set[str]:
        """Based on listed vcf file partition extract their basenames.

        # NOTE: Do not reconstruct full path to the mount, as it will
        # reduce the payload send to the google batch job. The mount
        # name is the same at every task command, the basename is
        # different.

        Returns:
            Set[str]: set of basenames to pass to the task environments.
        """
        input_path = self.pm.paths["input"]
        c = Client(project=self.project_id)
        b = c.bucket(bucket_name=input_path.bucket)
        blobs = b.list_blobs(prefix=input_path.path, match_glob="**.vcf")
        vcf_paths = {Path(blob.name).name for blob in blobs}
        # FIXME: Apparently this operator logs are not appearing in the airflow UI.
        self.log.info("Found %s vcf files", len(vcf_paths))
        return vcf_paths

    @cached_property
    def _vep_command(self) -> list[str]:
        return [
            "-c",
            rf"vep --cache --offline --format vcf --force_overwrite \
                --no_stats \
                --dir_cache {self.pm.cache_dir} \
                --input_file {self.pm.input_dir}/$INPUT_FILE \
                --output_file {self.pm.output_dir}/$OUTPUT_FILE --json \
                --dir_plugins {self.pm.cache_dir}/VEP_plugins \
                --sift b \
                --polyphen b \
                --fasta {self.pm.cache_dir}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz \
                --mane_select \
                --appris \
                --hgvsg \
                --pick_order  mane_select,canonical \
                --per_gene \
                --uniprot \
                --check_existing \
                --exclude_null_alleles \
                --canonical \
                --plugin TSSDistance \
                --distance 500000 \
                --plugin LoF,loftee_path:{self.pm.cache_dir}/VEP_plugins,gerp_bigwig:{self.pm.cache_dir}/gerp_conservation_scores.homo_sapiens.GRCh38.bw,human_ancestor_fa:{self.pm.cache_dir}/human_ancestor.fa.gz,conservation_file:/opt/vep/loftee.sql \
                --plugin AlphaMissense,file={self.pm.cache_dir}/AlphaMissense_hg38.tsv.gz,transcript_match=1 \
                --plugin CADD,snv={self.pm.cache_dir}/CADD_GRCh38_whole_genome_SNVs.tsv.gz",
        ]


class ConvertVariantsToVcfOperator(BaseOperator):
    def __init__(
        self,
        tsv_files_glob: str,
        output_path: str,
        *args,
        chunk_size: int = 2000,
        project_id: str = GCP_PROJECT_GENETICS,
        **kwargs,
    ) -> None:
        """Custom operator to merge vcf header based tab separated files found under given pattern.

        This operator merges vcf files by:
        - "#CHROM": str, '#' as a first column row name after header
        - "POS": int,
        - "ID": str,
        - "REF": str,
        - "ALT": str,
        - "QUAL": str,
        - "FILTER": str,
        - "INFO": str,
        header fields, performs deduplication and sorting if requested and
        partitions the output file by the partitions with `chunk_size` number
        of variants in each.

        """
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.tsv_files_glob = tsv_files_glob
        self.output_path = output_path
        self.chunk_size = chunk_size
        self.sep = "\t"
        self.output_files: list[str] = []

    def _list_input_files(self) -> Set[str]:
        gcs_path = GCSPath(self.tsv_files_glob)
        c = Client(project=self.project_id)
        b = c.bucket(bucket_name=gcs_path.bucket)
        blobs = b.list_blobs(
            prefix=gcs_path.segments["prefix"],
            match_glob=gcs_path.segments["filename"],
        )
        files = {f"gs://{gcs_path.bucket}/{blob.name}" for blob in blobs}
        self.log.info("Found %s files", files)
        return files

    def _cleanup_output_dir(self):
        gcs_path = GCSPath(self.output_path)
        client_object = Client(project=self.project_id)
        bucket_object = client_object.bucket(bucket_name=gcs_path.bucket)
        output_path_blob = Blob(name=gcs_path.path, bucket=bucket_object)
        if output_path_blob.exists(client=client_object):
            self.log.warning("Output path %s exists, will attempt to drop it", gcs_path)
            blobs_to_delete = list(
                bucket_object.list_blobs(prefix=gcs_path.segments["prefix"])
            )
            bucket_object.delete_blobs(blobs=blobs_to_delete)
        return self

    @property
    def _vcf_columns(self) -> OrderedDict[str, Type]:
        return OrderedDict(
            {
                "#CHROM": str,
                "POS": int,
                "ID": str,
                "REF": str,
                "ALT": str,
                "QUAL": str,
                "FILTER": str,
                "INFO": str,
            }
        )

    @property
    def _sort_columns(self) -> list[str]:
        return ["#CHROM", "POS"]

    @property
    def _deduplicate_columns(self) -> list[str]:
        return ["#CHROM", "POS", "REF", "ALT"]

    def _read_input_files(self) -> ConvertVariantsToVcfOperator:
        raw_dfs = [
            pd.read_csv(file, sep=self.sep, dtype=self._vcf_columns)
            for file in self._list_input_files()
        ]
        for raw_df in raw_dfs:
            self.log.info("Size of variants df from source: %s", raw_df.shape[0])

        self.df = (
            pd.concat(raw_dfs)
            .drop_duplicates(subset=self._deduplicate_columns)
            .sort_values(by=self._sort_columns)
            .reset_index(drop=True)
        )
        self.log.info("Size of variants df after merge: %s", self.df.shape[0])
        return self

    def _write_output_files(self) -> ConvertVariantsToVcfOperator:
        chunks = 0
        self.output_files = []
        self.log.info("")
        for i in range(0, self.df.shape[0], self.chunk_size):
            partial_df = self.df[i : i + self.chunk_size]
            filename = f"{self.output_path}/chunk_{i + 1}-{i + self.chunk_size}.vcf"
            self.log.info(
                "Outputting chunk %s with variant range %s-%s to %s",
                chunks,
                i,
                i + self.chunk_size - 1,  # last element is not contained in the list
                filename,
            )
            partial_df.to_csv(filename, index=False, header=True, sep=self.sep)
            chunks += 1
            self.output_files.append(filename)
        expected_chunk_count = len(self.df) // self.chunk_size + 1
        if not (chunks == expected_chunk_count):
            raise ValueError("Expected %s, got %s chunks", expected_chunk_count, chunks)
        return self

    def execute(self, **_) -> list[str]:
        """Execute the operator."""
        return (
            self._cleanup_output_dir()
            ._read_input_files()
            ._write_output_files()
            .output_files
        )
