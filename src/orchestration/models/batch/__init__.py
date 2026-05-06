from pydantic import BaseModel

from orchestration.models.batch.runnable import RunnableSpec


class ManifestGeneratorSpec(BaseModel):
    """Parameter specification for specific BatchManifest generator.

    Attributes:
        runnable (RunnableSpec): Runnable specification for the BatchManifest generator.
        generator_options (dict[str, str] | None): Keyword arguments for the manifest generator.

    """

    runnable: RunnableSpec
    """Runnable specification for the BatchManifest generator."""

    generator_options: dict[str, str]
    """Keyword arguments for the manifest generator."""


class BatchIndexSpec(BaseModel):
    """Batch index specification.

    Attributes:
        pointer (str): Pointer to correct BatchManifest generator.
        max_task_count (int): Maximum number of tasks per batch job. If the total number

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
