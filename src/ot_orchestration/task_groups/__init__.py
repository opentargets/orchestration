"""Module to store task groups."""

from ot_orchestration.task_groups.gwas_catalog import (
    gwas_catalog_batch_processing,
    gwas_catalog_manifest_preparation,
)


__all__ = [
    "gwas_catalog_batch_processing",
    "gwas_catalog_manifest_preparation",
]
