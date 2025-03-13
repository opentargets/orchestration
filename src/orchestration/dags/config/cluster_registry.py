"""Module defining common logic to build multiple cluster definitions for a single part of the unified pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)

from orchestration.utils.common import GCP_PROJECT_PLATFORM
from orchestration.utils.dataproc import create_cluster, delete_cluster
from orchestration.utils.utils import create_cluster_name


@dataclass
class Cluster:
    """Stores the cluster name and management methods."""

    name: str
    create: DataprocCreateClusterOperator
    delete: DataprocDeleteClusterOperator


class ClusterRegistry:
    """A map from cluster type to a cluster object."""

    def __init__(self, clusters: dict[str, dict[str, Any]]):
        self.cluster_types = clusters
        self.clusters: dict[str, Cluster] = {}

    def _add_cluster(self, cluster_type: str) -> Cluster:
        """Add a cluster to the registry."""
        name = create_cluster_name(f"gentropy_{cluster_type}")
        self.clusters[cluster_type] = Cluster(
            name=name,
            create=create_cluster(
                task_id=f"create_cluster_{cluster_type}",
                cluster_name=name,
                project_id=GCP_PROJECT_PLATFORM,
                **self.cluster_types[cluster_type],
            ),
            delete=delete_cluster(
                task_id=f"delete_cluster_{cluster_type}",
                cluster_name=name,
                project_id=GCP_PROJECT_PLATFORM,
            ),
        )
        return self.clusters[cluster_type]

    def get_cluster(self, step_config: dict[str, Any]) -> Cluster:
        """Get the cluster for the given step configuration."""
        cluster_type = step_config.get("cluster_type", "default")
        if cluster_type not in self.clusters:
            return self._add_cluster(cluster_type)
        return self.clusters[cluster_type]
