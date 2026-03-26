"""Airflow boilerplate code that interfaces with Batch operators which can be shared by several DAGs."""

from typing import Any

from google.cloud.batch_v1 import (
    GCS,
    AllocationPolicy,
    ComputeResource,
    Environment,
    Job,
    LifecyclePolicy,
    LogsPolicy,
    Runnable,
    TaskGroup,
    TaskSpec,
    Volume,
)

from orchestration.types import BatchPolicySpecs, BatchResourceSpecs, BatchTaskSpecs, GCSMountObject
from orchestration.utils import convert_params_to_hydra_positional_arg, time_to_seconds
from orchestration.utils.labels import Labels


def create_container_runnable(image: str, *, commands: list[str], **kwargs: Any) -> Runnable:
    """Create a container runnable for a Batch job with additional optional parameters.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        **kwargs (Any): Additional optional parameters to set on the container.

    Returns:
        Runnable: The container runnable.
    """
    return Runnable(container=Runnable.Container(image_uri=image, commands=commands, **kwargs))


def create_task_spec(
    image: str,
    commands: list[str],
    resource_specs: BatchResourceSpecs,
    task_specs: BatchTaskSpecs,
    lifecycle_policies: list[LifecyclePolicy] | None = None,
    **kwargs: Any,
) -> TaskSpec:
    """Create a task for a Batch job.

    Args:
        image (str): The Docker image to use.
        commands (list[str]): The commands to run in the container.
        resource_specs (BatchResourceSpecs): The specification of the resources for the task.
        task_specs (BatchTaskSpecs): The specification of the task.
        lifecycle_policies (list[LifecyclePolicy] | None) : Lifecycle policies.
        **kwargs (Any): Any additional parameter to pass to the container runnable

    Returns:
        TaskSpec: The task specification.
    """
    time_duration = time_to_seconds(task_specs["max_run_duration"])
    # See https://docs.cloud.google.com/batch/docs/troubleshooting#reserved-exit-codes
    default_lifecycle_policies = [
        LifecyclePolicy(
            action=LifecyclePolicy.Action.RETRY_TASK,
            action_condition=LifecyclePolicy.ActionCondition(exit_codes=[50001, 50002, 50003, 50004, 50005]),
        ),
    ]
    parameters = {
        "runnables": [create_container_runnable(image, commands=commands, **kwargs)],
        "compute_resource": ComputeResource(
            cpu_milli=resource_specs["cpu_milli"],
            memory_mib=resource_specs["memory_mib"],
            boot_disk_mib=resource_specs["boot_disk_mib"],
        ),
        "max_retry_count": task_specs["max_retry_count"],
        "max_run_duration": f"{time_duration}s",  # type: ignore
    }
    parameters.setdefault("lifecycle_policies", lifecycle_policies or default_lifecycle_policies)
    return TaskSpec(**parameters)


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
    parallelism: int | None = None,
    mounting_points: list[GCSMountObject] | None = None,
    labels: Labels | None = None,
) -> Job:
    """Create a Google Batch job.

    Args:
        task (TaskSpec): The task specification.
        task_env (list[Environment]): The environment variables for the task.
        policy_specs (BatchPolicySpecs): The policy specification for the task.
        parallelism (int | None): The number of tasks to run in parallel.
        mounting_points (list[GCSMountObject] | None): List of mounting points.
        labels (dict[str, str] | None): Labels for the job

    Returns:
        Job: The Batch job.
    """
    labels = labels or Labels()

    provisioning_model = policy_specs.get("provisioning_model", "SPOT")

    if mounting_points:
        task.volumes = set_up_mounting_points(mounting_points)

    task_spec_params = {"task_spec": task, "task_environments": task_env}
    if parallelism:
        task_spec_params["parallelism"] = parallelism
    return Job(
        task_groups=[TaskGroup(**task_spec_params)],
        allocation_policy=AllocationPolicy(
            instances=[
                AllocationPolicy.InstancePolicyOrTemplate(
                    policy=AllocationPolicy.InstancePolicy(
                        machine_type=policy_specs["machine_type"],
                        provisioning_model=AllocationPolicy.ProvisioningModel[provisioning_model],
                    )
                )
            ],
            labels=dict(labels),
        ),
        logs_policy=LogsPolicy(destination=LogsPolicy.Destination.CLOUD_LOGGING),
    )


def create_task_env(var_list: list[dict[str, str]]):
    """This function creates list of batch_v1.Environment objects from provided list of dictionaries."""
    return [Environment(variables=variables) for variables in var_list]


def create_task_commands(commands: list[str] | None, params: dict[str, str] | None) -> list[str]:
    """This function prepares list of commands for google batch job from the step configuration."""
    task_commands = []
    args: list[str] = []
    if params:
        args = convert_params_to_hydra_positional_arg(params=params, dataproc=False)
    if commands:
        task_commands.extend(commands)
    task_commands.extend(args)
    # ensure all are string values
    task_commands = [str(t) for t in task_commands]

    if len(task_commands) > 1 and task_commands[0] == "-c":
        task_commands = ["-c", " ".join(task_commands[1:])]
    return task_commands
