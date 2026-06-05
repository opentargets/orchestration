"""Models representing Google Batch operator-config mapping."""

from functools import cached_property
from typing import Annotated

from pydantic import BaseModel, StringConstraints

from orchestration.models.batch.environment import EnvironmentRegistrySpec
from orchestration.models.batch.job import JobSpec
from orchestration.utils.path import GCSPath


class BatchCollectSpec(BaseModel):
    """Specification for collecting PySpark nested output into a flat GCS directory.

    Attributes:
        source_prefix: GCS prefix where PySpark writes nested partition subdirectories.
        destination_prefix: GCS prefix where flat, renamed parquet files will land.
        file_glob: Glob pattern for files to collect. Defaults to ``**.parquet``.

    Examples:
        >>> spec = BatchCollectSpec(
        ...     source_prefix="gs://src-bucket/input/partitioned",
        ...     destination_prefix="gs://dst-bucket/output/flat",
        ... )
        >>> spec.file_glob
        '**.parquet'
    """

    source_prefix: Annotated[str, StringConstraints(pattern="^gs://.+/.+$")]
    """Source GCS prefix where the google batch task writes nested partition subdirectories."""
    destination_prefix: Annotated[str, StringConstraints(pattern="^gs://.+/.+$")]
    """GCS prefix where flat, renamed parquet files will land."""
    file_glob: Annotated[str, StringConstraints(pattern=r"^\*\*\.(parquet|tsv|tsv\.gz|csv|csv\.gz|json|jsonl)$")] = (
        "**.parquet"
    )
    """Glob pattern for files to collect. Defaults to ``**.parquet``. Should follow the format ``**.<extension>`` where extension is one of parquet, tsv, tsv.gz, csv, csv.gz, json, or jsonl."""

    @cached_property
    def file_extension(self) -> str:
        """File extension derived from the file glob pattern.

        Returns:
            str: The file extension to be collected, derived from the file glob pattern.

        Examples:
            >>> BatchCollectSpec(
            ...     source_prefix="gs://bucket/input",
            ...     destination_prefix="gs://bucket/output",
            ... ).file_extension
            'parquet'
            >>> BatchCollectSpec(
            ...     source_prefix="gs://bucket/input",
            ...     destination_prefix="gs://bucket/output",
            ...     file_glob="**.tsv.gz",
            ... ).file_extension
            'tsv.gz'
        """
        return self.file_glob.removeprefix("**.")

    @cached_property
    def destination_path(self) -> GCSPath:
        """Destination path for collected files, derived from the destination prefix.

        Returns:
            GCSPath: The destination path where collected files will be stored.

        Examples:
            >>> spec = BatchCollectSpec(
            ...     source_prefix="gs://src-bucket/input",
            ...     destination_prefix="gs://dst-bucket/output/flat/",
            ... )
            >>> spec.destination_path.bucket
            'dst-bucket'
            >>> spec.destination_path.path
            'output/flat'
        """
        return GCSPath(self.destination_prefix.removesuffix("/"))

    @cached_property
    def source_path(self) -> GCSPath:
        """Source path for files to collect, derived from the source prefix.

        Returns:
            GCSPath: The source path where files to be collected are located.

        Examples:
            >>> spec = BatchCollectSpec(
            ...     source_prefix="gs://src-bucket/input/partitioned/",
            ...     destination_prefix="gs://dst-bucket/output",
            ... )
            >>> spec.source_path.bucket
            'src-bucket'
            >>> spec.source_path.path
            'input/partitioned'
        """
        return GCSPath(self.source_prefix.removesuffix("/"))


class BatchIndexRow(BaseModel):
    """Representation of a single row in the batch index. Each row corresponds to a single batch job.

    a Pydantic model representing a single batch job, carrying a
    zero-based index and the `EnvironmentRegistrySpec` (one environment per task)
    for that job.
    """

    idx: int
    """Index of the batch job. This can be used to create unique identifiers for batch jobs and their tasks."""
    environments: EnvironmentRegistrySpec
    """Environment specification for the batch job. This will be used to create the Environment object for each task."""


class ManifestGeneratorSpec(BaseModel):
    """Parameter specification for specific BatchManifest generator.

    Attributes:
        generator_options (dict[str, str] | None): Keyword arguments for the manifest generator.

    """

    generator_options: dict[str, str]
    """Keyword arguments for the manifest generator."""


class BatchIndexOperatorSpec(BaseModel):
    """Batch index specification.

    Attributes:
        pointer (str): Pointer to correct BatchManifest generator.
        max_task_count (int): Maximum number of tasks per batch job.

    """

    pointer: str
    """Pointer to correct BatchManifest generator."""

    max_task_count: int
    """Maximum number of tasks per batch job.
        If the total number of tasks exceeds this limit, the
        batch index will be partitioned into multiple batch jobs.

        Each batch job will have at most `max_task_count` tasks,
        except for the last one which may have fewer.
    """

    generator_specs: ManifestGeneratorSpec
    """Generator specification for the BatchManifest generator."""


class BatchJobOperatorSpec(BaseModel):
    """Batch job specification.

    Attributes:
        job (JobSpec): Specification for the batch job.
        collect (BatchCollectSpec | None): Optional collect spec. When set, a
            ``BatchCollectOperator`` will sweep ``source_prefix`` after all batch
            jobs finish and copy matched files to ``collected_output`` with
            deterministic UUID5 names.
    """

    job: JobSpec
    """Specification for the batch job."""

    collect: BatchCollectSpec | None = None
    """Optional collect spec. When None the collect task is a no-op."""
