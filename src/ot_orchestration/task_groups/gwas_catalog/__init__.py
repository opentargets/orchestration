"""All task groups for GWAS Catalog processing."""

from ot_orchestration.task_groups.gwas_catalog.manifest_preparation import (
    gwas_catalog_manifest_preparation,
)
from ot_orchestration.task_groups.gwas_catalog.batch_processing import (
    gwas_catalog_batch_processing,
)

__all__ = [
    "gwas_catalog_manifest_preparation",
    "gwas_catalog_batch_processing",
]
