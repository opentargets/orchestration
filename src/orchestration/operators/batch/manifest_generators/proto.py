"""Manifest generator prototype.

Abstract representation of the concept of `Manifest Generator` for batch jobs.

This is a class that deals with the parallelization of the tasks. The key functionality
is to generate a `BatchIndex` object that contains the distributed information about
batch task environments. Each environment maps to a single and unique batch task.


The class that implements the `ProtoManifestGenerator` must implement:
- `from_generator_config` method that is a constructor of the generator given the specification.
- `generate_batch_index` method that generates the `BatchIndex` object.
"""

from __future__ import annotations

from typing import Protocol

from orchestration.models.batch import ManifestGeneratorSpec
from orchestration.operators.batch.batch_index import BatchIndex


class ProtoManifestGenerator(Protocol):
    """Protocol for manifest generators."""

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> ProtoManifestGenerator:
        """Constructor for Manifest Generator given the specification."""
        raise NotImplementedError("Implement it in subclasses")

    def generate_batch_index(self) -> BatchIndex:
        """Generate batch index."""
        raise NotImplementedError("Implement it in subclasses")
