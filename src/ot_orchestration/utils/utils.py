"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from google.cloud.storage import Client
import yaml

if TYPE_CHECKING:
    from pathlib import Path


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


def read_yaml_config(config_path: Path) -> Any:
    """Parse a YAMl config file and do all necessary checks.

    Args:
        config_path (Path): Path to the YAML config file.

    Returns:
        Any: Parsed YAML config file.
    """
    assert config_path.exists(), f"YAML config path {config_path} does not exist."
    with open(config_path) as config_file:
        return yaml.safe_load(config_file)
