"""Utility functions to handle manifests."""

from __future__ import annotations

import re


def extract_study_id_from_path(path: str) -> str:
    """Extract study id from path.

    Args:
        path (str): path to extract study id from.

    Returns:
        str: study id.

    Raises:
        ValueError: when identifier is not found.
    """
    pattern = r"\/(GCST\d+)\/"
    pattern = re.compile(pattern)
    result = pattern.search(path)
    if not result:
        raise ValueError("Gwas Catalog identifier was not found in %s", path)
    return result.group(1)


__all__ = ["extract_study_id_from_path"]
