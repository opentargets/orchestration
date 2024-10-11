"""Batch Index."""

from __future__ import annotations

import logging
from typing import TypedDict

from google.cloud.batch import Environment

from ot_orchestration.utils.batch import create_task_commands, create_task_env
from airflow.exceptions import AirflowSkipException


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
        logging.info(
            "Constructing batch task commands from commands: %s and options: %s",
            self.commands,
            self.options,
        )
        commands = create_task_commands(self.commands, self.options)
        return commands

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
        logging.info(
            "Constructing batch environments from vars_list: %s", self.vars_list
        )
        if not self.vars_list:
            logging.warning(
                "Can not create Batch environments from empty variable list, skipping"
            )
            raise AirflowSkipException(
                "Can not create Batch environments from empty variable list"
            )
        environments = create_task_env(self.vars_list)
        print(f"{environments=}")
        return environments

    def serialize(self) -> BatchEnvironmentsSerialized:
        """Serialize batch environments."""
        return BatchEnvironmentsSerialized(vars_list=self.vars_list)

    @staticmethod
    def deserialize(data: BatchEnvironmentsSerialized) -> BatchEnvironments:
        """Deserialize batch environments."""
        return BatchEnvironments(vars_list=data["vars_list"])


class BatchIndexRow(TypedDict):
    idx: int
    command: BatchCommandsSerialized
    environment: BatchEnvironmentsSerialized


class BatchIndex:
    """Index of all batch jobs.

    This object contains paths to individual manifest objects.
    Each of the manifests will be a single batch job.
    Each line of the individual manifest is a representation of the batch job task.
    """

    def __init__(
        self,
        vars_list: list[dict[str, str]],
        options: dict[str, str],
        commands: list[str],
        max_task_count: int,
    ) -> None:
        self.vars_list = vars_list
        self.options = options
        self.commands = commands
        self.max_task_count = max_task_count
        self.vars_batches: list[BatchEnvironmentsSerialized] = []

    def partition(self) -> BatchIndex:
        """Partition batch index by N chunks taking into account max_task_count."""
        if not self.vars_list:
            msg = "BatchIndex can not partition variable list, as list is empty."
            logging.warning(msg)
            return self

        if self.max_task_count > len(self.vars_list):
            logging.warning(
                "BatchIndex will use only one partition due to size of the dataset being smaller then max_task_count %s < %s",
                len(self.vars_list),
                self.max_task_count,
            )
            self.max_task_count = len(self.vars_list)

        for i in range(0, len(self.vars_list), self.max_task_count):
            batch = self.vars_list[i : i + self.max_task_count]
            self.vars_batches.append(BatchEnvironmentsSerialized(vars_list=batch))

        logging.info("Created %s task list batches.", len(self.vars_batches))

        return self

    @property
    def rows(self) -> list[BatchIndexRow]:
        """Create the master manifest that will gather the information needed to create batch Environments."""
        rows: list[BatchIndexRow] = []
        logging.info("Preparing BatchIndexRows. Each row represents a batch job.")
        for idx, batch in enumerate(self.vars_batches):
            rows.append(
                {
                    "idx": idx + 1,
                    "command": BatchCommandsSerialized(
                        options=self.options, commands=self.commands
                    ),
                    "environment": batch,
                }
            )

        logging.info("Prepared %s BatchIndexRows", len(rows))
        if not rows:
            raise AirflowSkipException(
                "Empty BatchIndexRows will not allow to create batch task. Skipping downstream"
            )
        return rows

    def __repr__(self) -> str:
        """Get batch index string representation."""
        return f"BatchIndex(vars_list={self.vars_list}, options={self.options}, commands={self.commands}, max_task_count={self.max_task_count})"
