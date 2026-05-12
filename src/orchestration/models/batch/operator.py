"""Models representing Google Batch operator-config mapping."""

from pydantic import BaseModel

from orchestration.models.batch.environment import EnvironmentRegistrySpec
from orchestration.models.batch.job import JobSpec


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
        batch_index_spec (BatchIndexSpec): Specification for the batch index.
    """

    job: JobSpec
    """Specification for the batch job."""
