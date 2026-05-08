"""Job specification for batch orchestration."""

from __future__ import annotations

from google.cloud import batch_v1
from pydantic import BaseModel

from orchestration.models.batch.environment import EnvironmentRegistrySpec
from orchestration.models.batch.instance import AllocationSpec
from orchestration.models.batch.logs import LogsSpec
from orchestration.models.batch.task_group import TaskGroupSpec
from orchestration.utils.labels import Labels


class JobSpec(BaseModel):
    task_group: TaskGroupSpec
    """Specification for the task groups to be executed in the batch job."""

    allocation: AllocationSpec
    """Specification for the allocation of resources for the batch job."""

    logs: LogsSpec
    """Specification for the logging configuration of the batch job."""

    labels: dict[str, str] | None = None
    """Labels to be applied to the batch job."""

    def build(
        self, task_environments: EnvironmentRegistrySpec | None = None, labels: dict[str, str] | None = None
    ) -> batch_v1.Job:
        """Build a `google.cloud.batch_v1.Job` object from the job specification.

        Args:
            task_environments: Optional environment registry to forward to the task group.
                Pass the partitioned ``EnvironmentRegistrySpec`` from a ``BatchIndexRow`` so
                that each submitted job receives its own slice of tasks.

            labels: Optional labels to override the labels defined in the JobSpec.
                This can be used to inject dynamic labels at runtime, for example by an Airflow operator.

        Returns:
            batch_v1.Job: The built Job object.

        Example:
        ---
        >>> from orchestration.models.batch.environment import EnvironmentSpec, EnvironmentRegistrySpec
        >>> from orchestration.models.batch.instance import AllocationSpec, InstanceSpec, InstanceResourceSpec
        >>> from orchestration.models.batch.runnable import RunnableSpec
        >>> from orchestration.models.batch.task import TaskConfiguration
        >>> from orchestration.models.batch.task_group import TaskGroupSpec
        >>> from orchestration.models.batch.logs import LogsSpec
        >>> irs = InstanceResourceSpec(cpu_milli=1000, memory_mib=2048, boot_disk_mib=51200)
        >>> rs = RunnableSpec(image_uri="gcr.io/p/img:latest", inline_commands=["echo", "hi"])
        >>> tc = TaskConfiguration(instance_resource_spec=irs, runnable_spec=rs)
        >>> envs = EnvironmentRegistrySpec(environments=[EnvironmentSpec(variables={"TASK_INDEX": "0"})])
        >>> tg = TaskGroupSpec(parallelism=1, task_config=tc, task_environments=envs)
        >>> alloc = AllocationSpec(instance=InstanceSpec(), labels={"team": "test"})
        >>> spec = JobSpec(task_group=tg, allocation=alloc, logs=LogsSpec(), labels={"team": "test"})
        >>> job = spec.build()
        >>> isinstance(job, batch_v1.Job)
        True
        >>> len(job.task_groups)
        1
        >>> job.allocation_policy.instances[0].policy.machine_type
        'n1-standard-4'
        >>> job.logs_policy.destination == batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
        True
        >>> dict(job.labels)
        {'team': 'test'}
        """
        j = {
            "task_groups": [self.task_group.build(task_environments=task_environments)],
            "allocation_policy": self.allocation.build(),
            "logs_policy": self.logs.build(),
            "labels": labels or self.labels or dict(Labels()),
        }

        return batch_v1.Job(**j)
