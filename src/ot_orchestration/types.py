"""Types introduced in the library."""

from typing import Literal

from typing_extensions import Required, TypedDict

ManifestObject = TypedDict(
    "ManifestObject",
    {
        "studyId": Required[str],
        "rawPath": Required[str],
        "harmonisedPath": Required[str],
        "passHarmonisation": bool | None,
        "passQC": bool | None,
        "qcPath": Required[str],
        "manifestPath": Required[str],
        "studyType": str | None,
        "analysisFlag": str | None,
        "isCurated": bool | None,
        "pubmedId": str | None,
        "status": Literal["success", "failure", "pending"],
    },
)


GCSMountObject = TypedDict(
    "GCSMountObject", {"remote_path": Required[str], "mount_point": Required[str]}
)

BatchTaskSpecs = TypedDict(
    "BatchTaskSpecs",
    {
        "max_retry_count": Required[int],
        "max_run_duration": Required[str],
    },
)

BatchResourceSpecs = TypedDict(
    "BatchResourceSpecs",
    {
        "cpu_milli": Required[int],
        "memory_mib": Required[int],
        "boot_disk_mib": Required[int],
    },
)

BatchPolicySpecs = TypedDict(
    "BatchPolicySpecs",
    {
        "machine_type": Required[str],
    },
)


BatchSpecs = TypedDict(
    "BatchSpecs",
    {
        "resource_specs": BatchResourceSpecs,
        "task_specs": BatchTaskSpecs,
        "policy_specs": BatchPolicySpecs,
        "image": str,
        "commands": list[str],
    },
)
