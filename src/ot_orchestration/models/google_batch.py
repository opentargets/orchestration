"""Google batch job specification models."""

from typing import Any

from pydantic import BaseModel


class BatchTaskSpecsModel(BaseModel):
    max_retry_count: int
    max_run_duration: str


class BatchResourceSpecsModel(BaseModel):
    cpu_milli: int
    memory_mib: int
    boot_disk_mib: int


class BatchPolicySpecsModel(BaseModel):
    machine_type: str


class GoogleBatchJobSpecs(BaseModel):
    resource_specs: BatchResourceSpecsModel
    task_specs: BatchTaskSpecsModel
    policy_specs: BatchPolicySpecsModel
    image: str
    commands: list[str]
    environment: list[dict[str, Any]]
    entrypoint: str


class ManifestGeneratorSpecsModel(BaseModel):
    commands: list[str]
    options: dict[str, str]
    manifest_kwargs: dict[str, str]


class GoogleBatchIndexSpecsModel(BaseModel):
    manifest_generator_label: str
    max_task_count: int
    manifest_generator_specs: ManifestGeneratorSpecsModel


class GoogleBatchSpecsModel(BaseModel):
    batch_index_specs: GoogleBatchIndexSpecsModel
    batch_job_specs: GoogleBatchJobSpecs
