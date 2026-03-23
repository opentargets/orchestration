"""Staging pipeline step class definition."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from orchestration.models.infrastructure.abc import InfrastructurePointer

type Env = Literal["Prod", "Test", "Dev"]


class EnvSpec(BaseModel):
    """Model representing the single environment specification ."""

    name: Env
    """Name of the environment, e.g. "Prod", "Test", etc."""
    vars: dict[str, str]
    """Dictionary of environment variable names and their corresponding values."""


class Environments(BaseModel):
    """Model representing all environments.

    This class implements the registry of environment specs with `get_vars` method to retrieve the templating context for a specific environment.
    """

    environments: list[EnvSpec]
    """List of environment specifications."""

    def get_vars(self, env_name: Env) -> dict[str, str]:
        """Get the environment variable values for the specified environment.

        Args:
            env_name: The environment to get the variable values for.

        Returns:
            A dictionary of environment variable names and their corresponding values for the specified environment.
        """
        for env_spec in self.environments:
            if env_spec.name == env_name:
                return env_spec.vars
        raise ValueError(f"Environment '{env_name}' not found in configuration.")
