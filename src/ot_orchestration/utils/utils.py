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

from ot_orchestration.types import ConfigNode, Environment, EnvironmentSpec


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


def create_name(step_name: str) -> str:
    """Create a google resource name for a given step name."""
    return f"up-{clean_name(step_name)}-{{{{ run_id | strhash }}}}"


def create_cluster_name(task_group_name: str) -> str:
    """Create a cluster name for a given task group name.

    The name will include our prefix `up-` and the task group name, so for the
    gentropy stage of run `3beef`, the cluster name will be:

    `up-gentropy-3beef`
    """
    return f"up-{clean_name(task_group_name)}-{{{{ run_id | strhash }}}}"


def read_yaml_config(
    config_path: Path | str,
    sentinels: dict[str, str] | None = None,
) -> Any:
    """Parse a YAML config file replacing sentinels.

    Args:
        config_path (Path | str): Path to the YAML config file.
        sentinels (dict[str, str] | None): Sentinels to replace in the config file.

    Sentinels in yaml files are in the form:

    `{variable_name}`

    Returns:
        Any: Parsed YAML config file.
    """
    config_path = config_path if isinstance(config_path, Path) else Path(config_path)
    assert config_path.exists(), f"YAML config path {config_path} does not exists"

    raw_config = config_path.read_text()
    if sentinels:
        for sentinel, replacement in sentinels.items():
            raw_config = raw_config.replace(f"{{{sentinel}}}", replacement)

    return yaml.safe_load(raw_config)


def to_yaml(config: dict) -> str:
    """Convert a dictionary to a YAML string."""
    return yaml.dump(config)


def read_hocon_config(
    config_path: Path | str,
    sentinels: dict[str, str] | None = None,
) -> Any:
    """Parse a HOCON config file replacing sentinels.

    Sentinels in hocon files are in the form:

    `{{variable_name}}`

    they are doubly enclosed in curly braces because hocon files use a single
    curly brace to denote a variable.

    Args:
        config_path (Path | str): Path to the HOCON config file.
        sentinels (dict[str, str] | None): Sentinels to replace in the config file.

    Returns:
        Any: Parsed HOCON config file.
    """
    config_path = config_path if isinstance(config_path, Path) else Path(config_path)
    assert config_path.exists(), f"HOCON config path {config_path} does not exists"

    raw_config = config_path.read_text()
    if sentinels:
        for sentinel, replacement in sentinels.items():
            raw_config = raw_config.replace(f"{{{{{sentinel}}}}}", replacement)

    return pyhocon.ConfigFactory.parse_string(raw_config)


def to_hocon(config: dict[str, Any]) -> str:
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
        node_dependencies = {node["id"]: node.get("prerequisites", []) for node in nodes}
        for label, node in tasks_or_task_groups.items():
            print(node_dependencies)
            for dependency in node_dependencies[label]:
                if dependency in tasks_or_task_groups:
                    node.set_upstream(tasks_or_task_groups[dependency])


def convert_params_to_hydra_positional_arg(params: dict[str, Any] | None, dataproc: bool = False) -> list[str]:
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


def find_node_in_config(config: list[ConfigNode], node_id: str) -> ConfigNode | None:
    """Find node config list."""
    for node_config in config:
        if node_config["id"] == node_id:
            return node_config
    return None


def find_environment_vars(env_spec: list[EnvironmentSpec], env: Environment) -> dict[str, str]:
    """Get the environment variables for a given environment."""
    for spec in env_spec:
        if spec["name"] == env:
            return spec["vars"]
    raise ValueError(f"Environment {env} not found in the environment specs")
