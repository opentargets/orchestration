"""Environment variable specifications for Google Batch tasks."""

from __future__ import annotations

from google.cloud import batch_v1
from pydantic import BaseModel


class EnvironmentSpec(BaseModel):
    """Environment variable(s) specification for a single task.

    Attributes:
        variables (dict[str, str]): Dictionary of environment variable(s) for the task.
    """

    variables: dict[str, str]
    """Dictionary of environment variable(s) for the task."""

    def build(self) -> batch_v1.Environment:
        """Build an Environment object from the environment variable specifications.

        Returns:
            batch_v1.Environment: An Environment object with the specified environment variables.

        Example:
        ---
        >>> spec = EnvironmentSpec(variables={"FOO": "bar", "BAZ": "qux"})
        >>> env = spec.build()
        >>> isinstance(env, batch_v1.Environment)
        True
        >>> env.variables == {"FOO": "bar", "BAZ": "qux"}
        True
        """
        return batch_v1.Environment(variables=self.variables)


class EnvironmentRegistrySpec(BaseModel):
    """Registry of environments.

    Attributes:
        environments (list[EnvironmentSpec]): List of environment variable(s). Each element of the list is
            an EnvironmentSpec object representing a single environment (all variable(s) for a single task).
            Size of the list will be equal to the number of tasks running.
    """

    environments: list[EnvironmentSpec]
    """List of environment variable(s). Each element of the list is
        an EnvironmentSpec object representing a single environment (all variable(s) for a single task).
        Size of the list will be equal to the number of tasks running.
    """

    def build(self) -> list[batch_v1.Environment]:
        """Build a list of Environment objects from the environment variable specifications.

        Returns:
            list[batch_v1.Environment]: The list of Environment objects with a single set of environment variables each.

        Example:
        ---
        >>> registry = EnvironmentRegistrySpec(environments=[
        ...     EnvironmentSpec(variables={"TASK_INDEX": "0"}),
        ...     EnvironmentSpec(variables={"TASK_INDEX": "1"}),
        ... ])
        >>> envs = registry.build()
        >>> len(envs)
        2
        >>> all(isinstance(e, batch_v1.Environment) for e in envs)
        True
        >>> envs[0].variables
        {'TASK_INDEX': '0'}
        >>> envs[1].variables
        {'TASK_INDEX': '1'}
        """
        return [env.build() for env in self.environments]

    def __len__(self) -> int:
        """Return the number of environments in the registry."""
        return len(self.environments)

    def __getitem__(self, idx: int) -> EnvironmentSpec:
        """Get the environment specification at the specified index."""
        return self.environments[idx]

    def partition(self, max_task_count: int) -> list[EnvironmentRegistrySpec]:
        """Partition the environment registry into batches of a specified maximum size.

        Args:
            max_task_count (int): The maximum number of tasks (environments) in each partition.

        Returns:
            list[EnvironmentRegistrySpec]: A list of EnvironmentRegistrySpec objects, each containing a partition of the environments.

        Example:
        ---
        >>> registry = EnvironmentRegistrySpec(environments=[
        ...     EnvironmentSpec(variables={"TASK_INDEX": "0"}),
        ...     EnvironmentSpec(variables={"TASK_INDEX": "1"}),
        ...     EnvironmentSpec(variables={"TASK_INDEX": "2"}),
        ... ])
        >>> partitions = registry.partition(max_task_count=2)
        >>> len(partitions)
        2
        >>> len(partitions[0].environments)
        2
        >>> len(partitions[1].environments)
        1
        >>> partitions = registry.partition(max_task_count=10)
        >>> len(partitions)
        1
        >>> len(partitions[0].environments)
        3
        """
        if self.empty:
            return []
        effective_max_task_count = min(max_task_count, len(self))
        return [
            EnvironmentRegistrySpec(environments=self.environments[i : i + effective_max_task_count])
            for i in range(0, len(self.environments), effective_max_task_count)
        ]

    @property
    def empty(self) -> bool:
        """Check if the environment registry is empty."""
        return len(self.environments) == 0

    def __repr__(self) -> str:
        """Get environment registry string representation."""
        return f"EnvironmentRegistrySpec(n={len(self.environments)} environments)"
