"""Types introduced in the library."""

from typing_extensions import Required, TypedDict
from typing import Literal


# type definitions
FTP_Transfer_Object = TypedDict(
    "FTP_Transfer_Object",
    {
        "source_path": Required[str],
        "destination_path": Required[str],
        "destination_bucket": Required[str],
    },
)

Manifest_Object = TypedDict(
    "Manifest_Object",
    {
        "studyId": Required[str],
        "rawPath": Required[str],
        "harmonisedPath": Required[str],
        "passHarmonisation": Required[bool] | Required[None],
        "passQC": Required[bool] | Required[None],
        "qcPath": Required[str],
        "manifestPath": Required[str],
        "studyType": Required[str] | Required[None],
        "analysisFlag": Required[str] | Required[None],
        "isCurated": Required[bool] | Required[None],
        "pubmetId": Required[str] | Required[None],
    },
)

Config_Field_Name = Literal["tags", "providers", "DAGS"]
Data_Source = Literal["GWAS_Catalog", "eQTL_Catalogque", "finngen", "UK_Biobank_PPP"]
ConfigFieldNotFound = str
Base_Type = str | list[str]
ConfigParsingFailure = str
JSON_blob = str
GCS_Mount_Object = TypedDict(
    "GCS_Mount_Object", {"remote_path": Required[str], "mount_point": Required[str]}
)

Batch_Task_Specs = TypedDict(
    "Batch_Task_Specs",
    {
        "max_retry_count": Required[int],
        "max_run_duration": Required[str],
    },
)

Batch_Resource_Specs = TypedDict(
    "Batch_Resource_Specs",
    {
        "cpu_milli": Required[int],
        "memory_mib": Required[int],
        "boot_disk_mib": Required[int],
    },
)

Batch_Policy_Specs = TypedDict(
    "Batch_Policy_Specs",
    {
        "machine_type": Required[str],
    },
)

Dataproc_Specs = TypedDict(
    "Dataproc_Specs",
    {
        "spark_uri": Required[Literal["yarn"]],
        "write_mode": Required[
            Literal["append", "overwrite", "error", "errorifexists", "ignore"]
        ],
    },
)

Batch_Specs = TypedDict(
    "Batch_Specs",
    {
        "resource_specs": Batch_Resource_Specs,
        "task_specs": Batch_Task_Specs,
        "policy_specs": Batch_Policy_Specs,
    },
)


__all__ = [
    "FTP_Transfer_Object",
    "Manifest_Object",
    "GCS_Mount_Object",
    "Data_Source",
    "Config_Field_Name",
    "ConfigFieldNotFound",
    "Base_Type",
    "ConfigParsingFailure",
    "JSON_blob",
    "Batch_Task_Specs",
    "Batch_Resource_Specs",
    "Dataproc_Specs",
    "Batch_Specs",
]
