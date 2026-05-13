from __future__ import annotations

import logging
from functools import cached_property

from airflow.exceptions import AirflowSkipException

from orchestration.models.batch import BatchIndexRow
from orchestration.models.batch.environment import EnvironmentRegistrySpec

logger = logging.getLogger(__name__)


class BatchIndex:
    """In-memory index of batch jobs produced by partitioning an EnvironmentRegistrySpec.

    Intended usage is a two-step sequence:
        1. Call `partition(max_task_count)` to split the registry into chunks, one per batch job.
        2. Access `rows` to get the resulting list of `BatchIndexRow` objects.

    `BatchIndex`: wraps an `EnvironmentRegistrySpec` and partitions it into chunks
    that each become a separate batch job. The intended call sequence is::

        index = generator.generate_batch_index()
        rows = index.partition(max_task_count).rows

    ``partition`` splits the registry into `EnvironmentRegistrySpec` chunks capped
    at ``max_task_count`` tasks each. ``rows`` converts those chunks into
    `BatchIndexRow` objects consumed by `BatchJobOperator`.
    """

    def __init__(
        self,
        env_registry: EnvironmentRegistrySpec,
    ) -> None:
        """Initialise a BatchIndex from an environment registry.

        Args:
            env_registry (EnvironmentRegistrySpec): Registry of environment specifications,
                one per task to be distributed across batch jobs.
        """
        self.environment_registry = env_registry
        self.env_batches: list[EnvironmentRegistrySpec] = []

    def partition(self, max_task_count: int) -> BatchIndex:
        """Partition the environment registry into chunks, each capped at `max_task_count` tasks.

        Args:
            max_task_count (int): Maximum number of tasks (environments) allowed per batch job.

        Returns:
            BatchIndex: self, enabling method chaining (e.g. ``index.partition(n).rows``).

        Raises:
            AirflowSkipException: If the environment registry is empty.
        """
        if self.environment_registry.empty:
            msg = "BatchIndex can not partition variable list, as list is empty."
            logger.warning(msg)
            raise AirflowSkipException(msg)

        self.env_batches = self.environment_registry.partition(max_task_count=max_task_count)
        logger.info("Created %s task list batches.", len(self.env_batches))

        return self

    @cached_property
    def rows(self) -> list[BatchIndexRow]:
        """Return one BatchIndexRow per partitioned batch job.

        Each row carries a zero-based index and the EnvironmentRegistrySpec for that job's tasks.
        Requires ``partition()`` to have been called first.

        Returns:
            list[BatchIndexRow]: Ordered list of batch job descriptors.

        Raises:
            AirflowSkipException: If no rows are available (e.g. ``partition()`` was never called).
        """
        rows: list[BatchIndexRow] = []
        logger.info("Preparing BatchIndexRows. Each row represents a batch job.")
        for idx, batch in enumerate(self.env_batches):
            rows.append(BatchIndexRow(idx=idx, environments=batch))

        logger.info("Prepared %s BatchIndexRows", len(rows))
        if not rows:
            raise AirflowSkipException("Empty BatchIndexRows will not allow to create batch task. Skipping downstream")
        return rows

    def __repr__(self) -> str:
        """Get batch index string representation."""
        return f"BatchIndex(env_registry={self.environment_registry})"
