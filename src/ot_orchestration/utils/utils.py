"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import yaml
from google.cloud.storage import Client


def bucket_name(gs_url: str) -> str:
    """Extract the bucket name from a GCS path."""
    return gs_url.replace("gs://", "").split("/", 1)[0]


def bucket_path(gs_url: str) -> str:
    """Extract the path from a GCS path."""
    return gs_url.replace("gs://", "").split("/", 1)[1]


def check_gcp_folder_exists(bucket_name: str, folder_path: str) -> bool:
    """Check if a folder exists in a Google Cloud bucket.

    Args:
        bucket_name (str): The name of the Google Cloud bucket.
        folder_path (str): The path of the folder to check.

    Returns:
        bool: True if the folder exists, False otherwise.
    """
    client = Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return any(blobs)


def create_name(prefix: str, suffix: str) -> str:
    """Create a clean name meeting google cloud naming conventions."""
    return re.sub(r"[^a-z0-9-]", "-", f"{prefix}-{suffix}".lower())


def read_yaml_config(config_path: Path | str) -> Any:
    """Parse a YAMl config file and do all necessary checks.

    Args:
        config_path (Path | str): Path to the YAML config file.

    Returns:
        Any: Parsed YAML config file.
    """
    config_path = config_path if isinstance(config_path, Path) else Path(config_path)
    assert config_path.exists(), f"YAML config path {config_path} does not exists"
    with open(config_path) as config_file:
        return yaml.safe_load(config_file)


def strhash(s: str) -> str:
    """Create a simple hash from a string."""
    return hashlib.sha256(s.encode()).hexdigest()[:5]


def time_to_seconds(time_str: str) -> int:
    """Parse time interval.

    Args:
        time_str (str): time string like 1d, 1h, 1m, 1s.

    Returns:
        int: time interval in seconds.

    Raises:
        ParsingError: when pattern is not matched.
    """
    time_pattern = r"^\d+[dhms]{1}$"
    result = re.match(time_pattern, time_str)
    if not result:
        raise ValueError("Cound not parse %s time string", time_str)
    match list(time_str):
        case [*days, "d"]:
            return int("".join(days)) * 24 * 60 * 60
        case [*hours, "h"]:
            return int("".join(hours)) * 60 * 60
        case [*minutes, "m"]:
            return int("".join(minutes)) * 60
        case [*seconds, "s"]:
            return int("".join(seconds))
        case _:
            return 0


__all__ = [
    "bucket_name",
    "bucket_path",
    "check_gcp_folder_exists",
    "create_name",
    "read_yaml_config",
    "strhash",
    "time_to_seconds",
]
