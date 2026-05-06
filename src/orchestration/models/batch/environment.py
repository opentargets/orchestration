"""Environment variable specifications for Google Batch tasks."""

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
