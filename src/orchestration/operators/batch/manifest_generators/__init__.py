"""Manifest generators."""

from __future__ import annotations

from typing import Protocol

from orchestration.operators.batch.batch_index import BatchIndex
from orchestration.types import ManifestGeneratorSpecs


class ProtoManifestGenerator(Protocol):
    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpecs) -> ProtoManifestGenerator:
        """Constructor for Manifest Generator given the specification."""
        raise NotImplementedError("Implement it in subclasses")

    def generate_batch_index(self) -> BatchIndex:
        """Generate batch index."""
        raise NotImplementedError("Implement it in subclasses")
