"""Batch Index Operator."""

from __future__ import annotations

from airflow.models.baseoperator import BaseOperator

from orchestration.models.batch import BatchIndexOperatorSpec, BatchIndexRow
from orchestration.operators.batch.manifest_generators import (
    FinemappingManifestGenerator,
    GentropyStepManifestGenerator,
    HarmonisationManifestGenerator,
    VepManifestGenerator,
)
from orchestration.operators.batch.manifest_generators.proto import ProtoManifestGenerator

MANIFEST_GENERATOR_MAP = {
    "vep": VepManifestGenerator,
    "gentropy_step": GentropyStepManifestGenerator,
    "finemapping": FinemappingManifestGenerator,
    "harmonisation": HarmonisationManifestGenerator,
}


class BatchIndexOperator(BaseOperator):
    """Operator to prepare google batch job index and partition it into the manifests.

    Each manifest prepared by the operator should create an environment for a single batch job.
    Each row of the individual manifest should represent individual batch task.
    """

    def __init__(
        self,
        batch_index_specs: BatchIndexOperatorSpec,
        **kwargs,
    ) -> None:
        self.manifest_generator = self.get_generator(batch_index_specs.pointer)
        self.generator_specs = batch_index_specs.generator_specs
        self.max_task_count = batch_index_specs.max_task_count
        super().__init__(**kwargs)

    @classmethod
    def get_generator(cls, label: str) -> type[ProtoManifestGenerator]:
        """Get the generator by it's label in the registry."""
        try:
            return MANIFEST_GENERATOR_MAP[label]
        except KeyError:
            raise KeyError(f"Manifest generator with label {label} not found in the manifest generator registry.")

    def execute(self, **kwargs) -> list[BatchIndexRow]:
        """Execute the operator."""
        generator = self.manifest_generator.from_generator_config(self.generator_specs)
        index = generator.generate_batch_index()
        if not self.max_task_count:
            # if specified 0 or not specified in the config, then assume to use the number
            # of tasks that is in the output of the BatchIndex.environment_registry from manifest generation
            self.max_task_count = len(index.environment_registry)
        self.log.info(index)
        partitioned_index = index.partition(self.max_task_count)
        return partitioned_index.rows
