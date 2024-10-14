"""Airflow boilerplate code which can be shared by several DAGs."""

from __future__ import annotations

import hashlib
import random
import re
import string
from pathlib import Path
from typing import Any

import pyhocon
import yaml
from google.cloud.storage import Client

from ot_orchestration.types import ConfigNode


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


def clean_name(name: str) -> str:
    """Create a clean name meeting google cloud naming conventions."""
    return re.sub(r"[^a-z0-9-]", "-", name.lower())


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


def to_yaml(config: dict) -> str:
    """Convert a dictionary to a YAML string."""
    return yaml.dump(config)


def read_hocon_config(config_path: Path | str) -> Any:
    """Parse a HOCON config file and do all necessary checks.

    Args:
        config_path (Path | str): Path to the HOCON config file.

    Returns:
        Any: Parsed HOCON config file.
    """
    config_path = config_path if isinstance(config_path, Path) else Path(config_path)
    assert config_path.exists(), f"HOCON config path {config_path} does not exists"
    with open(config_path) as config_file:
        return pyhocon.ConfigFactory.parse_string(config_file.read())


def to_hocon(config: pyhocon.ConfigTree) -> str:
    """Convert a ConfigTree to a HOCON string."""
    return pyhocon.HOCONConverter.to_hocon(config)


def strhash(s: str) -> str:
    """Create a simple hash from a string."""
    return hashlib.sha256(s.encode()).hexdigest()[:5]


def random_id(length: int = 5) -> str:
    """Create a random string of a given length."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


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


def chain_dependencies(nodes: list[ConfigNode], tasks_or_task_groups: dict[str, Any]):
    """Compare two dictionaries left containing task definitions and right containing tasks.

    Map the dependencies between tasks.

    """
    if nodes:
        node_dependencies = {
            node["id"]: node.get("prerequisites", []) for node in nodes
        }
        for label, node in tasks_or_task_groups.items():
            print(node_dependencies)
            for dependency in node_dependencies[label]:
                node.set_upstream(tasks_or_task_groups[dependency])


def convert_params_to_hydra_positional_arg(
    params: dict[str, Any] | None, dataproc: bool = False
) -> list[str]:
    """Convert configuration parameters to form that can be passed to hydra step positional arguments.

    This function parses to get the overwrite syntax used by hydra.
    https://hydra.cc/docs/advanced/override_grammar/basic/. Parameter keys have to start with `step.`
    The first parameter should be the step: "step_name".

    Args:
        params (dict[str, Any]] | None): Parameters for the step to convert.
        dataproc (bool): If true, adds the yarn as a session parameter.

    Raises:
        ValueError: When keys passed to the function params dict does not contain the `step.` prefix.

    Returns:
        list[str] | None: List of strings that represents the positional arguments for hydra gentropy step.
    """
    if not params:
        raise ValueError("Expected at least one parameter with the step: 'step_name'")
    incorrect_param_keys = [key for key in params if "step" not in key]
    if incorrect_param_keys:
        raise ValueError(f"Passed incorrect param keys {incorrect_param_keys}")
    positional_args = [f"{k}={v}" for k, v in params.items()]
    if not dataproc:
        return positional_args
    yarn_session_config = "step.session.spark_uri=yarn"
    if yarn_session_config not in positional_args:
        positional_args.append(yarn_session_config)
    return positional_args


def find_node_in_config(config: list[ConfigNode], node_id: str) -> ConfigNode:
    """Find node config list."""
    for node_config in config:
        if node_config["id"] == node_id:
            return node_config
    raise KeyError(f"Config for {node_id} was not found")
