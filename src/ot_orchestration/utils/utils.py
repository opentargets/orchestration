"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

from configparser import ParsingError
from typing import TYPE_CHECKING, Any
from google.cloud.storage import Client
import subprocess
import yaml
import re

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


def read_yaml_config(config_path: Path | str) -> Any:
    """Parse a YAMl config file and do all necessary checks.

    Args:
        config_path (Path | str): Path to the YAML config file.

    Returns:
        Any: Parsed YAML config file.
    """
    config_path = config_path if isinstance(config_path, Path) else Path(config_path)
    assert config_path.exists(), f"YAML config path {config_path} does not exist."
    with open(config_path) as config_file:
        return yaml.safe_load(config_file)


def get_bash_script_blob(path: Path | str) -> str:
    """Read and validate bash script as a string.

    The function utilizes the
    -n flag  - Read commands but do not execute them.  This may
                be used to check a shell script for syntax errors.
                This is ignored by interactive shells.
    https://www.man7.org/linux/man-pages/man1/bash.1.html

    Args:
        path (Path | str) either a path or a string to the script.

    Returns:
        str: validated command string.

    Raises:
        ParsingError: when script failed validation.
    """
    # dry run the script
    path = path if isinstance(path, Path) else Path(path)
    assert path.exists(), f"Bash script {path} does not exist."
    result = subprocess.run(["bash" "-n", str(path)], capture_output=True)
    if result.returncode != 0:
        raise ParsingError("Failed to parse script under %s", str(path))
    with open(path, "r") as f:
        content = f.read()
        return content


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
    match time_str:
        case [*days, "d"]:
            return int(days) * 24 * 60 * 60
        case [*hours, "h"]:
            return int(hours) * 60 * 60
        case [*minutes, "m"]:
            return int(minutes) * 60
        case [*seconds, "s"]:
            return int(seconds)
        case _:
            return 0


__all__ = [
    "check_gcp_folder_exists",
    "read_yaml_config",
    "get_bash_script_blob",
    "time_to_seconds",
]
