"""All task groups for GWAS Catalog processing."""

from ot_orchestration.task_groups.gwas_catalog.manifest_preparation import (
    gwas_catalog_manifest_preparation,
)
from ot_orchestration.task_groups.gwas_catalog.harmonisation import (
    gwas_catalog_harmonisation,
)

__all__ = [
    "gwas_catalog_manifest_preparation",
    "gwas_catalog_harmonisation",
]
