"""Utility functions to handle manifests."""

import re


def extract_study_id_from_path(path: str) -> str:
    """Extract study id from path."""
    pattern = re.compile("\/(GCST\d+)\/")
    result = pattern.search(path)
    if not result:
        raise ValueError("Gwas Catalog identifier was not found in %s", path)
    return result.group(1)


__all__ = ["extract_study_id_from_path"]
