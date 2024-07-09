"""Airflow boilerplate code that interfaces with Batch operators which can be shared by several DAGs."""

from typing import Any

from google.cloud import batch_v1

MACHINES = {
    "VEPMACHINE": {
        "machine_type": "e2-standard-4",
        "cpu_milli": 2000,
        "memory_mib": 2000,
        "boot_disk_mib": 10000,
    },
}

def create_container_runnable(
    image: str, commands: list[str], **kwargs: Any
) -> batch_v1.Runnable:
    """Create a container runnable for a Batch job with additional optional parameters.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        **kwargs (Any): Additional optional parameters to set on the container.

    Returns:
        batch_v1.Runnable: The container runnable.
    """
    container = batch_v1.Runnable.Container(
        image_uri=image, entrypoint="/bin/sh", commands=commands, **kwargs
    )
    return batch_v1.Runnable(container=container)


def create_task_spec(
    image: str, commands: list[str], **kwargs: Any
) -> batch_v1.TaskSpec:
    """Create a task for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        **kwargs (Any): Any additional parameter to pass to the container runnable

    Returns:
        batch_v1.TaskSpec: The task specification.
    """
    task = batch_v1.TaskSpec()
    task.runnables = [create_container_runnable(image, commands, **kwargs)]
    return task


def set_up_mounting_points(
    mounting_points: list[dict[str, str]],
) -> list[batch_v1.Volume]:
    """Set up the mounting points for the container.

    Args:
        mounting_points (list[dict[str, str]]): The mounting points.

    Returns:
        list[batch_v1.Volume]: The volumes.
    """
    volumes = []
    for mount in mounting_points:
        gcs_bucket = batch_v1.GCS()
        gcs_bucket.remote_path = mount["remote_path"]
        gcs_volume = batch_v1.Volume()
        gcs_volume.gcs = gcs_bucket
        gcs_volume.mount_path = mount["mount_point"]
        volumes.append(gcs_volume)
    return volumes


def create_batch_job(
    task: batch_v1.TaskSpec,
    machine: str,
    task_env: list[batch_v1.Environment],
    mounting_points: list[dict[str, str]] | None = None,
) -> batch_v1.Job:
    """Create a Google Batch job.

    Args:
        task (batch_v1.TaskSpec): The task specification.
        machine (str): The machine type to use.
        task_env (list[batch_v1.Environment]): The environment variables for the task.
        mounting_points (list[dict[str, str]] | None): List of mounting points.

    Returns:
        batch_v1.Job: The Batch job.
    """
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = MACHINES[machine]["cpu_milli"]
    resources.memory_mib = MACHINES[machine]["memory_mib"]
    resources.boot_disk_mib = MACHINES[machine]["boot_disk_mib"]
    task.compute_resource = resources

    task.max_retry_count = 3
    task.max_run_duration = "43200s"

    # The mounting points are set up and assigned to the task:
    task.volumes = set_up_mounting_points(mounting_points) if mounting_points else None

    group = batch_v1.TaskGroup()
    group.task_spec = task
    group.task_environments = task_env

    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = MACHINES[machine]["machine_type"]
    policy.provisioning_model = "SPOT"

    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    return job
