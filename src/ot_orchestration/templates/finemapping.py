"""A reusable template for finemapping jobs."""

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
            image_url=docker_image_url,
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
                    "step.purity_mean_r2_threshold=0 "
                    "step.purity_min_r2_threshold=0 "
                    "step.cs_lbf_thr=2 step.sum_pips=0.99 "
                    "step.susie_est_tausq=False "
                    "step.run_carma=False "
                    "step.run_sumstat_imputation=False "
                    "step.carma_time_limit=600 "
                    "step.imputed_r2_threshold=0.9 "
                    "step.ld_score_threshold=5 "
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
        resources=ComputeResource(cpu_milli=4000, memory_mib=25000),
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
