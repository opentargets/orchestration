"""Manifest generator for finemappping."""

from __future__ import annotations

from ot_orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from ot_orchestration.types import ManifestGeneratorSpecs


class FinemappingManifestGenerator(ProtoManifestGenerator):
    def __init__(self, *, commands, options, manifest_kwargs):
        pass

    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpecs) -> FinemappingManifestGenerator:
        """Create a FinemappingManifestGenerator from the specification."""
        return cls(
            commands=specs["commands"],
            options=specs["options"],
            manifest_kwargs=specs["manifest_kwargs"],
        )
