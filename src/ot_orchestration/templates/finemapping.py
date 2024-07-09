from google.cloud.batch_v1 import (
    AllocationPolicy,
    ComputeResource,
    Environment,
    LifecyclePolicy,
    LogsPolicy,
    TaskSpec,
    TaskGroup,
    Runnable,
    Job,
)

import common_airflow as common


def finemapping_batch_job(
    study_locus_paths: list[str],
    output_paths: list[str],
    study_index_path: str,
    docker_image_url: str = common.GENTROPY_DOCKER_IMAGE,
) -> Job:
    """Create a Batch job to run fine-mapping on a list of study loci.

    Args:
        study_locus_paths (list[str]): The list of study loci (full gs:// paths) to fine-map.
        study_index_path (str): The path to the study index.
        output_path (str): The path to store the output.
        output_path_log (str): The path to store the finemapping logs.
        docker_image_url (str): The URL of the Docker image to use for the job. By default, use a project wide image.

    Returns:
        Job: A Batch job to run fine-mapping on the given study loci.
    """
    # Check that the input parameters make sense.
    assert len(study_locus_paths) == len(
        output_paths
    ), "The length of study_locus_paths and output_paths must be the same."

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
                    'step.study_locus_to_finemap="$INPUTPATH" '
                    'step.output_path="$OUTPUTPATH" '
                    f"step.study_index_path={study_index_path} "
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

    # Define task environments: individual configuration for each of the tasks.
    task_environments = [
        Environment(variables={"INPUTPATH": input_path, "OUTPUTPATH": output_path})
        for input_path, output_path in zip(study_locus_paths, output_paths)
    ]

    # Define task group: collection of parameterised tasks.
    task_group = TaskGroup(
        task_spec=task_spec,
        task_environments=task_environments,
        parallelism=2000,
        task_count=len(study_locus_paths),
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
