"""Batch Job Operator."""

from __future__ import annotations

import time

from airflow.providers.google.cloud.operators.cloud_batch import CloudBatchSubmitJobOperator

from orchestration.models.batch import BatchIndexRow, BatchJobOperatorSpec
from orchestration.utils.common import GCP_PROJECT_GENETICS, GCP_REGION
from orchestration.utils.labels import Labels


class BatchJobOperator(CloudBatchSubmitJobOperator):
    """Generic Batch Job operator.

    This operator has to be used in conjunction to the BatchIndexOperator.
    It runs the google batch jobs defined by the BatchIndexOperator.
    """

    def __init__(
        self,
        job_name: str,
        batch_index_row: BatchIndexRow,
        batch_job_spec: BatchJobOperatorSpec,
        project_id: str = GCP_PROJECT_GENETICS,
        region: str = GCP_REGION,
        labels: Labels | None = None,
        **kwargs,
    ):
        job = batch_job_spec.job.build(
            task_environments=batch_index_row.environments, labels=dict(labels) if labels else None
        )
        super().__init__(
            project_id=project_id,
            region=region,
            job_name=f"{job_name}-job-{batch_index_row.idx}-{time.strftime('%Y%m%d-%H%M%S')}",
            job=job,
            deferrable=False,
            **kwargs,
        )
