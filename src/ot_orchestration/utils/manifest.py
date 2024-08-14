"""Utility functions to handle manifests."""

from __future__ import annotations
import re
from ot_orchestration.types import Manifest_Object
from ot_orchestration.utils import IOManager
import logging


def extract_study_id_from_path(path: str) -> str:
    """Extract study id from path.

    Args:
        path: str: path to extract study id from.

    Returns:
        str: study id.
    """
    pattern = r"\/(GCST\d+)\/"
    pattern = re.compile(pattern)
    result = pattern.search(path)
    if not result:
        raise ValueError("Gwas Catalog identifier was not found in %s", path)
    return result.group(1)


class GWASCatalogPipelineManifest:
    """Create Manifest object for gwas catalog study.

    Args:
        manifest_object (Manifest_Object): manifest object.
    """

    def __init__(self, manifest_object: Manifest_Object):
        self._mo = manifest_object

    def __getitem__(self, key: str) -> str:
        """Get one of manifest keys.

        Args:
            key (str): key to get.

        Returns:
            str: value of the key.
        """
        return self._mo[key]

    def __setitem__(self, key: str, val: str | bool) -> None:
        """Set one of the manifest keys.

        Args:
            key (str): key to set.
            val (str | bool): value to set.
        """
        self._mo[key] = val

    @classmethod
    def from_file(cls, mp: str) -> GWASCatalogPipelineManifest:
        """Constructor from a file.

        File can be either google cloud storage or local file.

        Args:
            mp (str): path to the manifest file.

        Returns:
            GWASCatalogPipelineManifest: manifest object.
        """
        if not mp.endswith(".json"):
            raise IOError("Manifest needs to be a json file")
        path = IOManager().resolve(mp)
        manifest: Manifest_Object = path.load()
        return cls(manifest)

    def write(self) -> None:
        """Write manifest object to the path derived from manifestPath."""
        mp = self._mo["manifestPath"]
        logging.info("WRITING MANIFEST OBJECT TO %s", mp)
        path = IOManager().resolve(mp)
        path.dump(self._mo)

    def __repr__(self) -> str:
        """Representation of the manifest object.

        Returns:
            str: inlined dict representation.
        """
        return str(self._mo)


__all__ = ["extract_study_id_from_path", "GWASCatalogPipelineManifest"]
