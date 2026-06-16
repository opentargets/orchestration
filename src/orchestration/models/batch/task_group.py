"""Task group definition for Google Batch tasks."""

from __future__ import annotations

from google.cloud import batch_v1
from pydantic import BaseModel

from orchestration.models.batch.environment import EnvironmentRegistrySpec
from orchestration.models.batch.task import TaskConfiguration


class TaskGroupSpec(BaseModel):
    """Class representing a group of tasks to be executed in a Google Batch job."""

    parallelism: int
    """Number of tasks to run in parallel."""

    task_count_per_node: int = 1
    """Number of tasks to run per node at the same time.
        By default, it is set to 1, meaning only one task will run on a node at a time."""

    task_config: TaskConfiguration
    """Specification for the tasks in the group."""

    task_environments: EnvironmentRegistrySpec
    """List of environment variable(s) for the tasks in the group.
        Each environment variable is represented as an EnvironmentRegistrySpec object.
        Size of the list will be equal to the number of tasks running.
        Each task will get one element from the list and pass it to environment.
    """

    def build(self, task_environments: EnvironmentRegistrySpec | None = None) -> batch_v1.TaskGroup:
        """Build a TaskGroup object from the TaskGroupSpec.

        Args:
            task_environments: Optional environment registry to use instead of ``self.task_environments``.
                Callers such as ``BatchJobOperator`` supply the partitioned environments produced by
                ``BatchIndexOperator`` here so that each submitted job gets its own task slice.

        Returns:
            batch_v1.TaskGroup: The built TaskGroup object.

        Example:
        ---
        >>> from orchestration.models.batch.environment import EnvironmentSpec, EnvironmentRegistrySpec
        >>> from orchestration.models.batch.instance import InstanceResourceSpec
        >>> from orchestration.models.batch.runnable import RunnableSpec
        >>> from orchestration.models.batch.task import TaskConfiguration
        >>> irs = InstanceResourceSpec(cpu_milli=1000, memory_mib=2048, boot_disk_mib=51200)
        >>> rs = RunnableSpec(image_uri="gcr.io/p/img:latest", inline_commands=["echo", "hi"])
        >>> tc = TaskConfiguration(instance_resource_spec=irs, runnable_spec=rs)
        >>> envs = EnvironmentRegistrySpec(environments=[
        ...     EnvironmentSpec(variables={"TASK_INDEX": "0"}),
        ...     EnvironmentSpec(variables={"TASK_INDEX": "1"}),
        ... ])
        >>> tg_spec = TaskGroupSpec(parallelism=2, task_config=tc, task_environments=envs)
        >>> tg = tg_spec.build()
        >>> isinstance(tg, batch_v1.TaskGroup)
        True
        >>> tg.parallelism
        2
        >>> tg.task_count_per_node
        1
        >>> len(tg.task_environments)
        2
        >>> dict(tg.task_environments[0].variables)
        {'TASK_INDEX': '0'}
        """
        effective_environments = task_environments if task_environments is not None else self.task_environments
        return batch_v1.TaskGroup(
            parallelism=self.parallelism,
            task_spec=self.task_config.build(),
            task_count_per_node=self.task_count_per_node,
            task_environments=effective_environments.build(),
        )
