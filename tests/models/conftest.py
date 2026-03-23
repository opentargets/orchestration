"""Fixtures for model tests."""

import importlib.resources
from importlib.resources.abc import Traversable
from pathlib import Path

import yaml


def staging_pipeline_config_paths() -> dict[str, Traversable]:
    """Staging pipeline configuration paths for testing."""
    data_dir = importlib.resources.files("orchestration.dags.config")

    return {"decode_ingestion": (data_dir / "decode_ingestion.yaml")}


def dummy_cluster_config(tmp_path: Path) -> Path:
    """Creates a dummy cluster config file for testing."""
    cluster_config = {
        "project_id": "test-project",
        "region": "test-region",
        "zone": "test-zone",
        "master_machine_type": "n1-standard-4",
        "worker_machine_type": "n1-standard-4",
        "num_workers": 2,
    }
    cluster_config_path = tmp_path / "cluster_config.yaml"
    with open(cluster_config_path, "w") as f:
        yaml.dump(cluster_config, f)
    return cluster_config_path
