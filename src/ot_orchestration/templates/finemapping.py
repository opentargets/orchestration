"""A reusable template for finemapping jobs."""

import time

from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)
from google.cloud import storage
from google.cloud.batch_v1 import (
    AllocationPolicy,
    ComputeResource,
    Job,
    LifecyclePolicy,
    LogsPolicy,
    Runnable,
    TaskGroup,
    TaskSpec,
)
from ot_orchestration.utils import common


def finemapping_batch_job(
    study_index_path: str,
    study_locus_manifest_path: str,
    task_count: int,
    docker_image_url: str = common.GENTROPY_DOCKER_IMAGE,
) -> Job:
    """Create a Batch job to run fine-mapping based on an input-output manifest.

    Args:
        study_index_path (str): The path to the study index.
        study_locus_manifest_path (str): Path to the CSV manifest containing all study locus input and output locations. Should contain two columns: study_locus_input and study_locus_output
        task_count (int): Total number of tasks in a job to run.
        docker_image_url (str): The URL of the Docker image to use for the job. By default, use a project wide image.

    Returns:
        Job: A Batch job to run fine-mapping on the given study loci.
    """
    # Define runnable: container and parameters to use.
    runnable = Runnable(
        container=Runnable.Container(
            image_uri=docker_image_url,
            entrypoint="/bin/sh",
            commands=[
                "-c",
                (
                    "poetry run gentropy "
                    "step=susie_finemapping "
                    f"step.study_index_path={study_index_path} "
                    f"step.study_locus_manifest_path={study_locus_manifest_path} "
                    "step.study_locus_index=$BATCH_TASK_INDEX "
                    "step.max_causal_snps=10 "
                    "step.primary_signal_pval_threshold=1 "
                    "step.secondary_signal_pval_threshold=1 "
                    "step.purity_mean_r2_threshold=0.25 "
                    "step.purity_min_r2_threshold=0 "
                    "step.cs_lbf_thr=2 step.sum_pips=0.99 "
                    "step.susie_est_tausq=False "
                    "step.run_carma=False "
                    "step.run_sumstat_imputation=False "
                    "step.carma_time_limit=600 "
                    "step.imputed_r2_threshold=0.9 "
                    "step.ld_score_threshold=5 "
                    "step.carma_tau=0.15 "
                    "+step.session.extended_spark_conf={spark.jars:https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar} "
                    "+step.session.extended_spark_conf={spark.dynamicAllocation.enabled:false} "
                    "+step.session.extended_spark_conf={spark.driver.memory:30g} "
                    "+step.session.extended_spark_conf={spark.kryoserializer.buffer.max:500m} "
                    "+step.session.extended_spark_conf={spark.driver.maxResultSize:5g} "
                    "step.session.write_mode=overwrite"
                ),
            ],
            options="-e HYDRA_FULL_ERROR=1",
        )
    )

    # Define task spec: runnable, compute resources, retry and lifecycle policies; shared between all tasks.
    task_spec = TaskSpec(
        runnables=[runnable],
        compute_resource=ComputeResource(cpu_milli=4000, memory_mib=25000),
        max_run_duration="7200s",
        max_retry_count=5,
        lifecycle_policies=[
            LifecyclePolicy(
                action=LifecyclePolicy.Action.FAIL_TASK,
                action_condition=LifecyclePolicy.ActionCondition(
                    exit_codes=[50005]  # Execution time exceeded.
                ),
            )
        ],
    )

    # Define task group: collection of parameterised tasks.
    task_group = TaskGroup(
        task_spec=task_spec,
        parallelism=2000,
        task_count=task_count,
    )

    # Define allocation policy: method of mapping a task group to compute resources.
    allocation_policy = AllocationPolicy(
        instances=[
            AllocationPolicy.InstancePolicyOrTemplate(
                policy=AllocationPolicy.InstancePolicy(
                    machine_type="n2-highmem-4",
                    provisioning_model=AllocationPolicy.ProvisioningModel.SPOT,
                    boot_disk=AllocationPolicy.Disk(size_gb=60),
                )
            )
        ]
    )

    # Define and return job: a complete description of the workload, ready to be submitted to Google Batch.
    return Job(
        task_groups=[task_group],
        allocation_policy=allocation_policy,
        logs_policy=LogsPolicy(destination=LogsPolicy.Destination.CLOUD_LOGGING),
    )


def upload_strings_to_gcs(strings_list: list[str], csv_upload_path: str) -> None:
    """Upload a list of strings directly to Google Cloud Storage as a single blob.

    Args:
        strings_list (List[str]): The list of strings to be uploaded.
        csv_upload_path (str): The full Google Storage path (gs://bucket_name/path/to/file.csv) where the data will be uploaded.

    Returns:
        None
    """
    # Join the list of strings with newlines to form the content.
    content = "\n".join(strings_list)

    # Extract bucket and path from csv_upload_path (format: gs://bucket_name/path/to/file.csv).
    bucket_name, file_path = csv_upload_path.replace("gs://", "").split("/", 1)

    # Initialise the Google Cloud Storage client.
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)

    # Upload the joined content directly.
    blob.upload_from_string(content, content_type="text/plain")


def generate_manifests_for_finemapping(
    collected_loci: str,
    manifest_prefix: str,
    output_path: str,
    max_records_per_chunk: int = 100_000,
) -> list[(int, str, int)]:
    """Starting from collected_loci, generate manifests for finemapping, splitting in chunks of at most 100,000 records.

    Args:
        collected_loci (str): Google Storage path for collected loci.
        manifest_prefix (str): Google Storage path prefix for uploading the manifests.
        output_path (str): Google Storage path to store the finemapping results.
        max_records_per_chunk (int): Maximum number of records per one chunk. Defaults to 100,000, which is the maximum number of tasks per job that Google Batch supports.

    Return:
        list[(int, str, int)]: List of tuples, where the first value is index of the manifest, the second value is a path to manifest, and the third is the number of records in that manifest.
    """
    # Get list of loci from the input Google Storage path.
    client = storage.Client()
    bucket_name, prefix = collected_loci.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    all_loci = [
        blob.name[:-1].split("/")[-1]
        for blob in blobs
        if "studyLocusId" in blob.name and blob.name.endswith("/")
    ]

    # Generate full list of input-output file paths.
    inputs_outputs = [
        f"{collected_loci}/{locus},{output_path}/{locus}" for locus in all_loci
    ]

    # Split into chunks of max size, as specified.
    split_inputs_outputs = [
        inputs_outputs[i : i + max_records_per_chunk]
        for i in range(0, len(inputs_outputs), max_records_per_chunk)
    ]

    # Generate and upload manifests.
    all_manifests = []
    for i, chunk in enumerate(split_inputs_outputs):
        lines = ["study_locus_input,study_locus_output"] + chunk
        manifest_path = f"{manifest_prefix}/chunk_{i}"
        upload_strings_to_gcs(lines, manifest_path)
        all_manifests.append(
            (i, manifest_path, len(chunk)),
        )

    return all_manifests


class FinemappingBatchOperator(CloudBatchSubmitJobOperator):
    def __init__(self, manifest: list[int, str, int], study_index_path: str, **kwargs):
        i, manifest_path, num_of_tasks = manifest
        super().__init__(
            project_id=common.GCP_PROJECT,
            region=common.GCP_REGION,
            job_name=f"finemapping-job-{i}-{time.strftime('%Y%m%d-%H%M%S')}",
            job=finemapping_batch_job(
                study_index_path=study_index_path,
                study_locus_manifest_path=manifest_path,
                task_count=num_of_tasks,
            ),
            deferrable=False,
            **kwargs,
        )
