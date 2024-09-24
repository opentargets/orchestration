"""Types introduced in the library."""

from __future__ import annotations

from typing import Any, Literal, TypedDict


class ManifestObject(TypedDict):
    studyId: str
    rawPath: str
    harmonisedPath: str
    passHarmonisation: bool | None
    passQC: bool | None
    qcPath: str
    manifestPath: str
    studyType: str | None
    analysisFlag: str | None
    isCurated: bool | None
    pubmedId: str | None
    status: Literal["success", "failure", "pending"]


class GCSMountObject(TypedDict):
    remote_path: str
    mount_point: str


class BatchTaskSpecs(TypedDict):
    max_retry_count: int
    max_run_duration: str


class BatchResourceSpecs(TypedDict):
    cpu_milli: int
    memory_mib: int
    boot_disk_mib: int


class BatchPolicySpecs(TypedDict):
    machine_type: str


class GoogleBatchSpecs(TypedDict):
    resource_specs: BatchResourceSpecs
    task_specs: BatchTaskSpecs
    policy_specs: BatchPolicySpecs
    image: str
    commands: list[str]
    environment: list[dict[str, Any]]
    entrypoint: str


class DataprocSpecs(TypedDict):
    python_main_module: str
    cluster_init_script: str
    cluster_metadata: dict[str, str]
    cluster_name: str


class ConfigNode(TypedDict):
    id: str
    kind: Literal["Task", "TaskGroup"]
    prerequisites: list[str]
    params: dict[str, Any]
    google_batch: GoogleBatchSpecs
    nodes: list[ConfigNode]
