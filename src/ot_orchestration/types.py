"""Types introduced in the library."""

from typing import TypedDict
from typing import Literal

# type definitions
FTP_Transfer_Object = TypedDict(
    "SFTP_Transfer_Object",
    {
        "source_path": str,
        "destination_path": str,
        "destination_bucket": str,
    },
)
Provider_Name = Literal["dataproc", "googlebatch"]
Config_Field_Name = Literal["tags", "providers", "DAGS"]
Data_Source = Literal["GWAS_Catalog", "eQTL_Catalogque", "finngen", "UK_Biobank_PPP"]
ConfigFieldNotFound = str
Base_Type = str | int | float | bool | list[str]
Dag_Params = dict[str, dict[str, Base_Type | list[Base_Type]]]
ConfigParsingFailure = str
