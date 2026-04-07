"""Abstract base classes and shared models for Google Cloud infrastructure definitions.

This module provides the foundational building blocks for defining, registering,
and referencing cloud infrastructure within a pipeline. It is intentionally
infrastructure-agnostic: concrete implementations (e.g. Google Batch, Dataproc)
extend these classes to supply their own typed configuration models.

Key components:

- :class:`InfrastructureDefinition` - base model pairing an infrastructure type
  identifier with a typed configuration object.
- :class:`InfrastructureRegistry` - a named collection of
  :class:`InfrastructureDefinition` instances that can be validated and looked
  up by pipeline steps.
- :class:`InfrastructurePointer` - a lightweight reference used inside step
  configurations to resolve the correct registry entry at runtime.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Self

from pydantic import BaseModel

from orchestration.models.secret import Secret
from orchestration.utils import resource_name


class InfrastructureRegistry[ConfigT](BaseModel):
    """A named registry of infrastructure definitions of a common configuration type.

    Holds a mapping from user-defined names to :class:`InfrastructureDefinition`
    instances. Pipeline steps reference entries by name via an
    :class:`InfrastructurePointer`, and this registry is responsible for
    validating that all required definitions are present before a pipeline runs.
    """

    items: Mapping[str, InfrastructureDefinition[ConfigT]]
    """Mapping of infrastructure definition names to their corresponding definitions."""

    def ensure(self, required_definitions: set[str]) -> Self:
        """Validate that all required infrastructure definitions are present in the registry.

        Args:
            required_definitions: Set of definition names that must exist in the registry.

        Returns:
            Self: The registry instance, allowing method chaining.

        Raises:
            ValueError: If one or more required definition names are absent from the registry.
        """
        missing_clusters = required_definitions - self.items.keys()
        if missing_clusters:
            raise ValueError(f"Missing required cluster definitions in registry: {missing_clusters}")
        return self

    def get(self, infrastructure_name: str) -> InfrastructureDefinition[ConfigT]:
        """Retrieve an infrastructure definition by name.

        Args:
            infrastructure_name: Name of the infrastructure definition to retrieve.

        Returns:
            InfrastructureDefinition: The definition registered under the given name.

        Raises:
            ValueError: If no definition with the given name exists in the registry.
        """
        if infrastructure_name not in self.items:
            raise ValueError(f"Cluster definition '{infrastructure_name}' not found in registry.")
        return self.items[infrastructure_name]


class InfrastructureDefinition[ConfigT](BaseModel):
    """Base model for a named, typed infrastructure definition.

    Concrete subclasses bind a specific :attr:`infrastructure` type identifier
    (e.g. ``"GOOGLE_BATCH_JOB"``) to a typed :attr:`config` object and an
    optional list of :class:`~orchestration.models.secret.Secret` values that
    should be made available on the infrastructure machines.

    Subclasses should define:

    - :attr:`infrastructure` - a fixed string identifying the infrastructure type.
    - :attr:`config` - a concrete configuration model specific to that type.
    """

    infrastructure: str
    """Identifier for the infrastructure type (e.g. ``"GOOGLE_BATCH_JOB"``)."""

    name: str
    """Human-readable name for this infrastructure definition, used as the registry key."""

    secrets: list[Secret] | None = None
    """Optional list of secrets to inject into the infrastructure machines."""

    config: ConfigT
    """Typed configuration object specific to the concrete infrastructure implementation."""

    @property
    def resource_name(self) -> str:
        """Canonical resource name derived from :attr:`name`.

        Returns:
            str: A normalised resource name suitable for use in Google Cloud API calls.
        """
        return resource_name(self.name)
