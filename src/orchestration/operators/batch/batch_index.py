from __future__ import annotations

import logging
from functools import cached_property
from typing import TypedDict

from airflow.exceptions import AirflowSkipException
from google.cloud.batch import Environment

from orchestration.models.batch import BatchIndexRow
from orchestration.models.batch.environment import EnvironmentRegistrySpec
from orchestration.utils.batch import create_task_commands, create_task_env

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Legacy symbols — kept for backward compatibility with generic.py.
# Removed in PR3 (feat/batch-dag-migration) when generic.py is deleted.
# ---------------------------------------------------------------------------

class BatchCommandsSerialized(TypedDict):
    options: dict[str, str]
    commands: list[str]


class BatchEnvironmentsSerialized(TypedDict):
    vars_list: list[dict[str, str]]


class BatchCommands:
    def __init__(self, options: dict[str, str], commands: list[str]):
        self.options = options
        self.commands = commands

    def construct(self) -> list[str]:
        """Construct Batch commands from mapping."""
        return create_task_commands(self.commands, self.options)

    def serialize(self) -> BatchCommandsSerialized:
        """Serialize batch commands."""
        return BatchCommandsSerialized(options=self.options, commands=self.commands)

    @staticmethod
    def deserialize(data: BatchCommandsSerialized) -> BatchCommands:
        """Deserialize batch commands."""
        return BatchCommands(options=data["options"], commands=data["commands"])


class BatchEnvironments:
    def __init__(self, vars_list: list[dict[str, str]]):
        self.vars_list = vars_list

    def construct(self) -> list[Environment]:
        """Construct Batch Environment from list of mappings."""
        if not self.vars_list:
            raise AirflowSkipException("Can not create Batch environments from empty variable list")
        return create_task_env(self.vars_list)

    def serialize(self) -> BatchEnvironmentsSerialized:
        """Serialize batch environments."""
        return BatchEnvironmentsSerialized(vars_list=self.vars_list)

    @staticmethod
    def deserialize(data: BatchEnvironmentsSerialized) -> BatchEnvironments:
        """Deserialize batch environments."""
        return BatchEnvironments(vars_list=data["vars_list"])


class _LegacyBatchIndexRow(TypedDict):
    idx: int
    command: BatchCommandsSerialized
    environment: BatchEnvironmentsSerialized


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
