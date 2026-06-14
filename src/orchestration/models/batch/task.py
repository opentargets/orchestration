"""Task definition for Google Batch tasks."""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import ClassVar

from google.cloud import batch_v1
from pydantic import BaseModel

from orchestration.models.batch.environment import EnvironmentSpec
from orchestration.models.batch.instance import InstanceResourceSpec
from orchestration.models.batch.runnable import RunnableSpec
from orchestration.models.batch.volume import VolumeRegistrySpec
from orchestration.utils import time_to_seconds


# NOTE: `TaskSpec` is already reserved by `google.cloud.batch_v1.TaskSpec`.
class TaskConfiguration(BaseModel):
    """Batch task specifications.

    This class represents the specifications for a task to be executed in a Google Batch job.

    Attributes:
        max_retry_count (int): Maximum number of retries for the task in case of failures.
        max_run_duration (str): Maximum run duration for the task in ISO 8601 format (e.g., "3600s" for 1 hour).

    Example:
    ---
    >>> irs = InstanceResourceSpec(cpu_milli=2000, memory_mib=4096, boot_disk_mib=102400)
    >>> rs = RunnableSpec(image_uri="gcr.io/my-project/my-image:latest", inline_commands=["python", "script.py"])
    >>> task_spec = TaskConfiguration(instance_resource_spec=irs, runnable_spec=rs)
    >>> ts = task_spec.build()
    >>> isinstance(ts, batch_v1.TaskSpec)
    True
    >>> ts.compute_resource.cpu_milli
    2000
    >>> ts.compute_resource.memory_mib
    4096
    >>> ts.compute_resource.boot_disk_mib
    102400
    >>> ts.max_run_duration.seconds
    3600
    >>> ts.max_retry_count
    0
    >>> ts.lifecycle_policies[0].action == batch_v1.LifecyclePolicy.Action.RETRY_TASK
    True
    >>> list(ts.lifecycle_policies[0].action_condition.exit_codes)
    [50001, 50002, 50003, 50004, 50005]
    """

    # See https://docs.cloud.google.com/batch/docs/troubleshooting#reserved-exit-codes
    DEFAULT_EXIT_CODES: ClassVar[tuple[int, ...]] = (50001, 50002, 50003, 50004, 50005)

    instance_resource_spec: InstanceResourceSpec
    """Resource specifications (cpu, memory and disk) for the task."""

    runnable_spec: RunnableSpec
    """Runnable specification (script, commands and container image) for the task."""

    max_retry_count: int = 0
    """Maximum number of retries for the task in case of failures.
        By default, it is set to 0, meaning no retries will be attempted in case of task failure."""

    max_run_duration: str = "3600s"
    """Maximum run duration for the task in ISO 8601 format (e.g., '3600s' for 1 hour)."""

    exit_codes: Sequence[int] | None = None
    """Sequence of exit codes that should trigger a task retry according to the lifecycle policy.
        If not provided, the default exit codes are 50001, 50002, 50003, 50004, and 50005."""

    shared_environment: EnvironmentSpec | None = None
    """Optional shared environment variables to be included in the task specification.
        This can be potentially used to include shared environment variables to all tasks in the job.
    """
    shared_volumes: VolumeRegistrySpec | None = None
    """Optional shared volumes to be included in the task specification.
        This can be potentially used to include shared volumes to all tasks in the job.
    """

    @property
    def max_run_duration_seconds(self) -> int:
        """Get the maximum run duration in seconds."""
        return time_to_seconds(self.max_run_duration)

    @property
    def effective_exit_codes(self) -> list[int]:
        """Get the effective exit codes for the task."""
        return list(self.exit_codes) if self.exit_codes is not None else list(self.DEFAULT_EXIT_CODES)

    @property
    def lifecycle_policies(self) -> list[batch_v1.LifecyclePolicy]:
        """Get the lifecycle policies for the task based on the effective exit codes.

        Note:
            By default the lifecycle policy is set to `google.cloud.batch_v1.LifecyclePolicy.Action.RETRY_TASK`
            for the default exit codes (50001, 50002, 50003, 50004, and 50005).
            This means that **if the task exits with any of these exit codes**, it will be retried up to `max_retry_count` times.
            If `exit_codes` are provided, the lifecycle policy will be set to retry the task for those exit codes instead.
        """
        return [
            batch_v1.LifecyclePolicy(
                action=batch_v1.LifecyclePolicy.Action.RETRY_TASK,
                action_condition=batch_v1.LifecyclePolicy.ActionCondition(exit_codes=self.effective_exit_codes),
            )
        ]

    def build(
        self,
    ) -> batch_v1.TaskSpec:
        """Build a `google.cloud.batch_v1.TaskSpec` object from specification.

        Returns:
            batch_v1.TaskSpec: A `google.cloud.batch_v1.TaskSpec` object built from the provided specifications.
        """
        spec = {
            "runnables": [self.runnable_spec.build()],
            "compute_resource": self.instance_resource_spec.build(),
            "max_run_duration": datetime.timedelta(seconds=self.max_run_duration_seconds),
            "max_retry_count": self.max_retry_count,
            "lifecycle_policies": self.lifecycle_policies,
        }
        if self.shared_environment:
            spec["environment"] = self.shared_environment.build()
        if self.shared_volumes:
            spec["volumes"] = self.shared_volumes.build()

        return batch_v1.TaskSpec(**spec)
