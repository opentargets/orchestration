"""Backward-compat shim. Removed in feat/batch-dag-migration (PR3)."""

from orchestration.operators.batch.manifest_generators.gentropy_step import (
    GentropyStepManifestGenerator as GentropyStepGoogleBatchManifestGenerator,
)

__all__ = ["GentropyStepGoogleBatchManifestGenerator"]
