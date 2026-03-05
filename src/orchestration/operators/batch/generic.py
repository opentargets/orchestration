"""Batch Job main operators.

This module defines two generic Airflow operators used to submit
partitioned jobs to Google Batch.  The ``BatchIndexOperator`` builds
a manifest describing each task and writes the associated environment
variables, while the ``BatchJobOperator`` wraps the Google Batch
client to submit each individual task.

This file is a local snapshot of the corresponding module in the
OpenTargets ``orchestration`` repository.  It has been extended to
register an additional manifest generator for heritability estimation.
"""

from __future__ import annotations

import time

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.operators.cloud_batch import CloudBatchSubmitJobOperator

from orchestration.operators.batch.batch_index import BatchCommands, BatchEnvironments, BatchIndexRow
from orchestration.operators.batch.manifest_generators import ProtoManifestGenerator
from orchestration.operators.batch.manifest_generators.harmonisation import HarmonisationManifestGenerator
from orchestration.operators.batch.manifest_generators.l2g_prediction import GentropyStepGoogleBatchManifestGenerator
from orchestration.types import GoogleBatchIndexSpecs, GoogleBatchSpecs
from orchestration.utils.batch import create_batch_job, create_task_spec
from orchestration.utils.common import GCP_PROJECT_GENETICS, GCP_REGION
from orchestration.utils.labels import Labels

try:
    from orchestration.operators.batch.manifest_generators.heritability_estimate import HeritabilityManifestGenerator
except Exception:
    HeritabilityManifestGenerator = None  # type: ignore


class BatchIndexOperator(BaseOperator):
    """Operator to prepare google batch job index and partition it into the manifests.

    Each manifest prepared by the operator should create an environment for
    a single batch job.  Each row of the individual manifest should represent
    an individual batch task.
    """

    # NOTE: here register all manifest generators.
    manifest_generator_registry: dict[str, type[ProtoManifestGenerator]] = {
        "gwas_catalog_harmonisation": HarmonisationManifestGenerator,
        "gentropy-step": GentropyStepGoogleBatchManifestGenerator,
        "heritability-estimate": HeritabilityManifestGenerator,
    }

    def __init__(
        self,
        batch_index_specs: GoogleBatchIndexSpecs,
        **kwargs,
    ) -> None:
        self.generator_label = batch_index_specs["manifest_generator_label"]
        self.manifest_generator = self.get_generator(self.generator_label)
        self.manifest_generator_specs = batch_index_specs["manifest_generator_specs"]
        self.max_task_count = batch_index_specs.get("max_task_count", 0)
        super().__init__(**kwargs)

    @classmethod
    def get_generator(cls, label: str) -> type[ProtoManifestGenerator]:
        """Get the generator by its label in the registry."""
        try:
            return cls.manifest_generator_registry[label]
        except KeyError:
            raise KeyError(f"Manifest generator with label {label} not found in the manifest generator registry.")

    def execute(self, **kwargs) -> list[BatchIndexRow]:
        """Execute the operator."""
        generator = self.manifest_generator.from_generator_config(self.manifest_generator_specs)
        index = generator.generate_batch_index()
        if not self.max_task_count:
            # if specified 0 or not specified in the config, then assume to use
            # the number of tasks that is in the output of the BatchIndex.vars_list
            # from manifest generation
            self.max_task_count = len(index.vars_list)
        self.log.info(index)
        partitioned_index = index.partition(self.max_task_count)
        return partitioned_index.rows


class BatchJobOperator(CloudBatchSubmitJobOperator):
    """Generic Batch Job operator.

    This operator has to be used in conjunction with the BatchIndexOperator.
    It runs the google batch jobs defined by the BatchIndexOperator.
    """

    def __init__(
        self,
        job_name: str,
        batch_index_row: BatchIndexRow,
        google_batch: GoogleBatchSpecs,
        project_id: str = GCP_PROJECT_GENETICS,
        region: str = GCP_REGION,
        labels: Labels | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            project_id=project_id,
            region=region,
            job_name=f"{job_name}-job-{batch_index_row['idx']}-{time.strftime('%Y%m%d-%H%M%S')}",
            job=create_batch_job(
                task=create_task_spec(
                    image=google_batch["image"],
                    commands=BatchCommands.deserialize(batch_index_row["command"]).construct(),
                    task_specs=google_batch["task_specs"],
                    resource_specs=google_batch["resource_specs"],
                    entrypoint=google_batch["entrypoint"],
                ),
                task_env=BatchEnvironments.deserialize(batch_index_row["environment"]).construct(),
                policy_specs=google_batch["policy_specs"],
                labels=labels or Labels(project=project_id),
            ),
            deferrable=False,
            **kwargs,
        )
