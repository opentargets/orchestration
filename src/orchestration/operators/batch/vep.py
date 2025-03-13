"""VEP annotation operator."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import Sequence, Set

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from google.cloud.batch import JobStatus
from google.cloud.batch_v1 import Job
from google.cloud.storage import Client

from orchestration.types import GCSMountObject, GoogleBatchSpecs
from orchestration.utils.batch import (
    create_batch_job,
    create_task_env,
    create_task_spec,
)
from orchestration.utils.common import GCP_PROJECT_GENETICS, GCP_REGION
from orchestration.utils.labels import Labels
from orchestration.utils.path import GCSPath


class VepAnnotationPathManager:
    """Manager class for setting correct mounting points for VEP google batch tasks."""

    def __init__(
        self,
        vcf_input_path: str,
        vep_output_path: str,
        vep_cache_path: str,
        mount_dir_root: str,
    ):
        self._mount_dir_root = mount_dir_root
        self.paths = {
            "input": GCSPath(vcf_input_path),
            "output": GCSPath(vep_output_path),
            "cache": GCSPath(vep_cache_path),
        }

    @cached_property
    def mount_dir_root(self) -> str:
        """Get the mount directory root."""
        if not self._mount_dir_root.startswith("/"):
            raise ValueError("Mount dir has to be an absolute path.")
        if self._mount_dir_root.endswith("/"):
            return str(Path(self._mount_dir_root))
        return self._mount_dir_root

    @cached_property
    def path_registry(self) -> dict[str, GCSMountObject]:
        """Get the path registry."""
        return {
            key: {
                # NOTE: remote_path has to start from the bucket_name but without the gs://
                # see https://cloud.google.com/batch/docs/create-run-job-storage#gcloud_2:~:text=BUCKET_PATH%3A%20the%20path,the%20subdirectory%20subdirectory.
                "remote_path": f"{value.bucket}/{value.path}",
                "mount_point": f"{self.mount_dir_root}/{key}",
            }
            for key, value in self.paths.items()
        }

    @cached_property
    def cache_dir(self) -> str:
        """Get cache dir."""
        return self.path_registry["cache"]["mount_point"]

    @cached_property
    def input_dir(self) -> str:
        """Get input dir."""
        return self.path_registry["input"]["mount_point"]

    @cached_property
    def output_dir(self) -> str:
        """Get output dir."""
        return self.path_registry["output"]["mount_point"]

    @cached_property
    def mount_config(self) -> list[GCSMountObject]:
        """Return the mount configuration.

        Returns:
            list[dict[str, str]]: The mount configuration.
        """
        return list(self.path_registry.values())


class VepAnnotateOperator(GoogleCloudBaseOperator):
    """Annotate vcf files in batch job.

    This operator performs the VEP annotation of vcf files provided in the `vcf_input_path`.
    The annotation command is custom to OTG needs. The number of batch tasks is inferred by listing the number of
    vcf files introduced in the `vcf_input_path`. Each input file will result in a single vep task.

    The operator tries to list the tasks after they are completed, in case of any failures in the task, the
    operator throws AirflowException and stops the execution of the DAG. The defails of the failed job can be
    retrieved from the logs.
    """

    def __init__(
        self,
        job_name: str,
        vcf_input_path: str,
        vep_output_path: str,
        vep_cache_path: str,
        google_batch: GoogleBatchSpecs,
        mount_dir_root: str = "/mnt/disks/share",
        gcp_region: str = GCP_REGION,
        project_id: str = GCP_PROJECT_GENETICS,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        polling_period_seconds: float = 10,
        timeout_seconds: float | None = None,
        labels: Labels | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.job_name = job_name
        self.region = gcp_region

        self.vcf_input_path = vcf_input_path
        self.vep_output_path = vep_output_path
        self.vep_cache_path = vep_cache_path
        self.google_batch = google_batch
        self.mount_dir_root = mount_dir_root
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_period_seconds = polling_period_seconds
        self.timeout_seconds = timeout_seconds
        self.labels = labels or Labels()
        self.pm = VepAnnotationPathManager(
            vcf_input_path=self.vcf_input_path,
            vep_output_path=self.vep_output_path,
            vep_cache_path=self.vep_cache_path,
            mount_dir_root=self.mount_dir_root,
        )

    template_fields: Sequence[str] = (
        "job_name",
        "labels",
    )

    @cached_property
    def hook(self) -> CloudBatchHook:
        """Get the cloud batch hook."""
        return CloudBatchHook(self.gcp_conn_id, self.impersonation_chain)

    def execute(self, context) -> dict:
        """Execute the operator."""
        vcf_files = self._get_vcf_partition_basenames(self.pm.paths["input"])
        environments = [
            {"INPUT_FILE": file, "OUTPUT_FILE": file.replace(".csv", ".json")}
            for file in vcf_files
        ]
        run = context.get("params", {}).get("run_label", context.get("dag_run").run_id)
        self.labels.add({"run": run})

        job_def = create_batch_job(
            task=create_task_spec(
                image=self.google_batch["image"],
                commands=self._vep_command,
                resource_specs=self.google_batch["resource_specs"],
                task_specs=self.google_batch["task_specs"],
                entrypoint=self.google_batch["entrypoint"],
            ),
            task_env=create_task_env(environments),
            policy_specs=self.google_batch["policy_specs"],
            mounting_points=self.pm.mount_config,
            labels=self.labels,
        )
        self.log.debug(job_def)
        job = self.hook.submit_batch_job(
            job_name=self.job_name,
            job=job_def,
            region=self.region,
            project_id=self.project_id,
        )
        completed_job = self.hook.wait_for_job(
            job_name=job.name,
            polling_period_seconds=self.polling_period_seconds,
            timeout=self.timeout_seconds,
        )
        self.log.debug(completed_job)

        # Retrieve the job status
        _filter = f"name:projects/{self.project_id}/locations/{self.region}/jobs/{self.job_name}*"
        jobs = list(
            self.hook.list_jobs(
                region=self.region, project_id=self.project_id, filter=_filter
            )
        )
        if len(jobs) != 1:
            raise AirflowException(f"Found more then one job for id {self.job_name}")

        job_status = jobs[0].status
        job_state = job_status.state

        if job_state != JobStatus.State.SUCCEEDED:
            self.log.error(job_status)
            raise AirflowException(f"Job {self.job_name} failed.")

        return Job.to_dict(jobs[0])  # type: ignore

    def _get_vcf_partition_basenames(self, input_path: GCSPath) -> Set[str]:
        """Based on listed vcf file partition extract their basenames.

        NOTE: Do not reconstruct full path to the mount, as it will
        reduce the payload send to the google batch job. The mount
        name is the same at every task command, the basename is
        different.

        Returns:
            Set[str]: set of basenames to pass to the task environments.
        """
        c = Client(project=self.project_id)
        b = c.bucket(bucket_name=input_path.bucket)
        blobs = b.list_blobs(prefix=input_path.path, match_glob="**.csv")
        vcf_paths = {Path(blob.name).name for blob in blobs}
        # FIXME: Apparently this operator logs are not appearing in the airflow UI.
        self.log.info("Found %s vcf files", len(vcf_paths))
        return vcf_paths

    @cached_property
    def _vep_command(self) -> list[str]:
        return [
            "-c",
            # NOTE: Ensure the CHROM column is replaced with #CHROM
            rf"sed -i '0,/CHROM/s/CHROM/#CHROM/' {self.pm.input_dir}/$INPUT_FILE && \
                 vep \
                --cache \
                --offline \
                --format vcf \
                --force_overwrite \
                --no_stats \
                --dir_cache {self.pm.cache_dir} \
                --input_file {self.pm.input_dir}/$INPUT_FILE \
                --output_file {self.pm.output_dir}/$OUTPUT_FILE \
                --json \
                --dir_plugins {self.pm.cache_dir}/VEP_plugins \
                --sift b \
                --fasta {self.pm.cache_dir}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz \
                --mane_select \
                --appris \
                --hgvsg \
                --pick_order  mane_select,canonical \
                --per_gene \
                --uniprot \
                --symbol \
                --biotype \
                --check_existing \
                --exclude_null_alleles \
                --canonical \
                --plugin TSSDistance \
                --distance 500000 \
                --plugin Conservation,{self.pm.cache_dir}/gerp_conservation_scores.homo_sapiens.GRCh38.bw,MAX \
                --plugin LoF,loftee_path:{self.pm.cache_dir}/VEP_plugins,gerp_bigwig:{self.pm.cache_dir}/gerp_conservation_scores.homo_sapiens.GRCh38.bw,human_ancestor_fa:{self.pm.cache_dir}/human_ancestor.fa.gz,conservation_file:/opt/vep/loftee.sql \
                --plugin AlphaMissense,file={self.pm.cache_dir}/AlphaMissense_hg38.tsv.gz,transcript_match=1",
        ]
