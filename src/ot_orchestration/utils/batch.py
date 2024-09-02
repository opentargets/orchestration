"""Airflow boilerplate code that interfaces with Batch operators which can be shared by several DAGs."""

from typing import Any

from google.cloud.batch_v1 import (
    GCS,
    AllocationPolicy,
    ComputeResource,
    Environment,
    Job,
    LogsPolicy,
    Runnable,
    TaskGroup,
    TaskSpec,
    Volume,
)

from ot_orchestration.types import (
    BatchPolicySpecs,
    BatchResourceSpecs,
    BatchTaskSpecs,
    GCSMountObject,
)
from ot_orchestration.utils.utils import time_to_seconds


def create_container_runnable(
    image: str, *, commands: list[str], **kwargs: Any
) -> Runnable:
    """Create a container runnable for a Batch job with additional optional parameters.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        **kwargs (Any): Additional optional parameters to set on the container.

    Returns:
        Runnable: The container runnable.
    """
    runnable = Runnable(
        container=Runnable.Container(image_uri=image, commands=commands, **kwargs)
    )
    return runnable


def create_task_spec(
    image: str,
    commands: list[str],
    resource_specs: BatchResourceSpecs,
    task_specs: BatchTaskSpecs,
    **kwargs: Any,
) -> TaskSpec:
    """Create a task for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        resource_specs (BatchResourceSpecs): The specification of the resources for the task.
        task_specs (BatchTaskSpecs): The specification of the task.
        **kwargs (Any): Any additional parameter to pass to the container runnable

    Returns:
        TaskSpec: The task specification.
    """
    time_duration = time_to_seconds(task_specs["max_run_duration"])
    task = TaskSpec(
        runnables=[create_container_runnable(image, commands=commands, **kwargs)],
        compute_resource=ComputeResource(
            cpu_milli=resource_specs["cpu_milli"],
            memory_mib=resource_specs["memory_mib"],
            boot_disk_mib=resource_specs["boot_disk_mib"],
        ),
        max_retry_count=task_specs["max_retry_count"],
        max_run_duration=f"{time_duration}s",  # type: ignore
    )

    return task


def set_up_mounting_points(
    mounting_points: list[GCSMountObject],
) -> list[Volume]:
    """Set up the mounting points for the container.

    Args:
        mounting_points (list[GCSMountObject]): The mounting points.

    Returns:
        list[Volume]: The volumes.
    """
    volumes = []
    for mount in mounting_points:
        gcs_bucket = GCS(remote_path=mount["remote_path"])
        gcs_volume = Volume(gcs=gcs_bucket, mount_path=mount["mount_point"])
        volumes.append(gcs_volume)
    return volumes


def create_batch_job(
    task: TaskSpec,
    task_env: list[Environment],
    policy_specs: BatchPolicySpecs,
    mounting_points: list[GCSMountObject] | None = None,
) -> Job:
    """Create a Google Batch job.

    Args:
        task (TaskSpec): The task specification.
        task_env (list[Environment]): The environment variables for the task.
        policy_specs (BatchPolicySpecs): The policy specification for the task
        mounting_points (list[GCSMountObject] | None): List of mounting points.

    Returns:
        Job: The Batch job.
    """
    if mounting_points:
        task.volumes = set_up_mounting_points(mounting_points)

    job = Job(
        task_groups=[TaskGroup(task_spec=task, task_environments=task_env)],
        allocation_policy=AllocationPolicy(
            instances=[
                AllocationPolicy.InstancePolicyOrTemplate(
                    policy=AllocationPolicy.InstancePolicy(
                        machine_type=policy_specs["machine_type"],
                        provisioning_model=AllocationPolicy.ProvisioningModel.SPOT,
                    )
                )
            ]
        ),
        logs_policy=LogsPolicy(destination=LogsPolicy.Destination.CLOUD_LOGGING),
    )

    return job


__all__ = ["create_batch_job", "create_task_spec"]
