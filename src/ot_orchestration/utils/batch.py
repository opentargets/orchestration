"""Airflow boilerplate code that interfaces with Batch operators which can be shared by several DAGs."""

from typing import Any

from google.cloud.batch_v1 import (
    Runnable,
    TaskSpec,
    Volume,
    GCS,
    Environment,
    Job,
    ComputeResource,
    TaskGroup,
    AllocationPolicy,
    LogsPolicy,
)
from ot_orchestration.types import (
    GCS_Mount_Object,
    Batch_Policy_Specs,
    Batch_Task_Specs,
    Batch_Resource_Specs,
)
from google.protobuf.duration_pb2 import Duration
from ot_orchestration.utils import time_to_seconds


def create_container_runnable(
    image: str, *, commands: list[str], **kwargs: Any
) -> Runnable:
    """Create a container runnable for a Batch job with additional optional parameters.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        **kwargs (Any): Additional optional parameters to set on the container.

    Returns:
        batch_v1.Runnable: The container runnable.


    """
    runnable = Runnable()
    runnable.container = Runnable.Container(
        image_uri=image, commands=commands, **kwargs
    )
    return runnable


def create_task_spec(
    image: str,
    commands: list[str],
    resource_specs: Batch_Resource_Specs,
    task_specs: Batch_Task_Specs,
    **kwargs: Any,
) -> TaskSpec:
    """Create a task for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        resource_specs (Batch_Resource_Specs): The specification of the resources for the task.
        task_specs (Batch_Task_Specs): The specification of the task.
        **kwargs (Any): Any additional parameter to pass to the container runnable

    Returns:
        batch_v1.TaskSpec: The task specification.
    """
    task = TaskSpec()
    task.runnables = [create_container_runnable(image, commands=commands, **kwargs)]
    resources = ComputeResource()
    resources.cpu_milli = resource_specs["cpu_milli"]
    resources.memory_mib = resource_specs["memory_mib"]
    resources.boot_disk_mib = resource_specs["boot_disk_mib"]
    task.compute_resource = resources

    task.max_retry_count = task_specs["max_retry_count"]
    task.max_run_duration = Duration(
        seconds=time_to_seconds(task_specs["max_run_duration"])
    )

    return task


def set_up_mounting_points(
    mounting_points: list[GCS_Mount_Object],
) -> list[Volume]:
    """Set up the mounting points for the container.

    Args:
        mounting_points (list[GCS_Mount_Object]): The mounting points.

    Returns:
        list[batch_v1.Volume]: The volumes.
    """
    volumes = []
    for mount in mounting_points:
        gcs_bucket = GCS()
        gcs_bucket.remote_path = mount["remote_path"]
        gcs_volume = Volume()
        gcs_volume.gcs = gcs_bucket
        gcs_volume.mount_path = mount["mount_point"]
        volumes.append(gcs_volume)
    return volumes


def create_batch_job(
    task: TaskSpec,
    task_env: list[Environment],
    policy_specs: Batch_Policy_Specs,
    mounting_points: list[GCS_Mount_Object] | None = None,
) -> Job:
    """Create a Google Batch job.

    Args:
        task (TaskSpec): The task specification.
        task_env (list[Environment]): The environment variables for the task.
        policy_specs: (Batch_Policy_Specs): The policy specification for the task
        mounting_points (list[GCS_Mount_Object]] | None): List of mounting points.

    Returns:
        Job: The Batch job.
    """
    if mounting_points:
        task.volumes = set_up_mounting_points(mounting_points)

    group = TaskGroup()
    group.task_spec = task
    group.task_environments = task_env

    policy = AllocationPolicy.InstancePolicy()
    policy.machine_type = policy_specs["machine_type"]
    policy.provisioning_model = AllocationPolicy.ProvisioningModel.SPOT

    instances = AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy = AllocationPolicy()
    allocation_policy.instances = [instances]

    job = Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.logs_policy = LogsPolicy()
    job.logs_policy.destination = LogsPolicy.Destination.CLOUD_LOGGING

    return job


__all__ = ["create_batch_job", "create_task_spec"]
