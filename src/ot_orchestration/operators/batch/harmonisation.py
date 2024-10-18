"""Operators for batch job."""

from __future__ import annotations

import logging
import time
from typing import Type

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchSubmitJobOperator,
)

from ot_orchestration.operators.batch.batch_index import (
    BatchCommands,
    BatchEnvironments,
    BatchIndexRow,
)
from ot_orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from ot_orchestration.operators.batch.manifest_generators.harmonisation import (
    HarmonisationManifestGenerator,
)
from ot_orchestration.types import GoogleBatchIndexSpecs, GoogleBatchSpecs
from ot_orchestration.utils.batch import create_batch_job, create_task_spec
from ot_orchestration.utils.common import GCP_PROJECT_GENETICS, GCP_REGION

logging.basicConfig(level=logging.DEBUG)


class BatchIndexOperator(BaseOperator):
    """Operator to prepare google batch job index.

    Each manifest prepared by the operator should create an environment for a single batch job.
    Each row of the individual manifest should represent individual batch task.
    """

    # NOTE: here register all manifest generators.
    manifest_generator_registry: dict[str, Type[ProtoManifestGenerator]] = {
        "gwas_catalog_harmonisation": HarmonisationManifestGenerator
    }

    def __init__(
        self,
        batch_index_specs: GoogleBatchIndexSpecs,
        **kwargs,
    ) -> None:
        self.generator_label = batch_index_specs["manifest_generator_label"]
        self.manifest_generator = self.get_generator(self.generator_label)
        self.manifest_generator_specs = batch_index_specs["manifest_generator_specs"]
        self.max_task_count = batch_index_specs["max_task_count"]
        super().__init__(**kwargs)

    @classmethod
    def get_generator(cls, label: str) -> Type[ProtoManifestGenerator]:
        """Get the generator by it's label in the registry."""
        return cls.manifest_generator_registry[label]

    def execute(self, context) -> list[BatchIndexRow]:
        """Execute the operator."""
        generator = self.manifest_generator.from_generator_config(
            self.manifest_generator_specs, max_task_count=self.max_task_count
        )
        index = generator.generate_batch_index()
        self.log.info(index)
        partitioned_index = index.partition()
        rows = partitioned_index.rows
        return rows


class GeneticsBatchJobOperator(CloudBatchSubmitJobOperator):
    def __init__(
        self,
        job_name: str,
        batch_index_row: BatchIndexRow,
        google_batch: GoogleBatchSpecs,
        **kwargs,
    ):
        super().__init__(
            project_id=GCP_PROJECT_GENETICS,
            region=GCP_REGION,
            job_name=f"{job_name}-job-{batch_index_row['idx']}-{time.strftime('%Y%m%d-%H%M%S')}",
            job=create_batch_job(
                task=create_task_spec(
                    image=google_batch["image"],
                    commands=BatchCommands.deserialize(
                        batch_index_row["command"]
                    ).construct(),
                    task_specs=google_batch["task_specs"],
                    resource_specs=google_batch["resource_specs"],
                    entrypoint=google_batch["entrypoint"],
                ),
                task_env=BatchEnvironments.deserialize(
                    batch_index_row["environment"]
                ).construct(),
                policy_specs=google_batch["policy_specs"],
            ),
            deferrable=False,
            **kwargs,
        )
