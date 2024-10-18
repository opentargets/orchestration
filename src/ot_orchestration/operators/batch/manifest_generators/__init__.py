"""Manifest generators."""

from __future__ import annotations

from typing import Protocol

from ot_orchestration.operators.batch.batch_index import BatchIndex
from ot_orchestration.types import ManifestGeneratorSpecs


class ProtoManifestGenerator(Protocol):
    @classmethod
    def from_generator_config(
        cls, specs: ManifestGeneratorSpecs, max_task_count: int
    ) -> ProtoManifestGenerator:
        """Constructor for Manifest Generator."""
        raise NotImplementedError("Implement it in subclasses")

    def generate_batch_index(self) -> BatchIndex:
        """Generate batch index."""
        raise NotImplementedError("Implement it in subclasses")
