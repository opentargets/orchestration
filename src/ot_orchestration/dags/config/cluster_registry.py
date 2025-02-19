"""Module defining common logic to build multiple cluster definitions for a single part of the unified pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from ot_orchestration.utils.common import GCP_PROJECT_PLATFORM
from ot_orchestration.utils.dataproc import create_cluster, delete_cluster
from ot_orchestration.utils.utils import create_cluster_name


@dataclass
class Cluster:
    """Box class to store the tasks to build the dataproc cluster and the reference to it's name."""

    create: DataprocCreateClusterOperator
    delete: DataprocDeleteClusterOperator
    name: str


class ClusterRegistry:
    """CLuster registry object.

    This registry allows to build a dictionary of DataprocClusterOperator tasks:
     -  `DataprocCreateClusterOperator`
     -  `DataprocDeleteClusterOperator`
    """

    def __init__(self):
        self.clusters: dict[str, Cluster] = {}

    def _add_cluster(self, cluster_settings: dict):
        """Method to add cluster tasks to the cluster registry.

        The original name of the cluster - `cluster_name` is used as a key for the registry,
        the actual `cluster_name` is defined at a runtime with the `clean_cluster_name` function.
        """
        cluster_name = cluster_settings["cluster_name"]
        if not self.clusters.get(cluster_name):
            clean_name = create_cluster_name(cluster_name)
            cluster_settings.update({"cluster_name": clean_name})
            c = create_cluster(
                task_id=f"create_{cluster_name}",
                project_id=GCP_PROJECT_PLATFORM,
                **cluster_settings,
            )
            d = delete_cluster(
                task_id=f"delete_{cluster_name}",
                cluster_name=clean_name,
                project_id=GCP_PROJECT_PLATFORM,
            )
            self.clusters[cluster_name] = Cluster(create=c, delete=d, name=clean_name)
        return self

    @classmethod
    def from_dataproc_cluster_settings(cls, dataproc_cluster_settings: list[dict]) -> ClusterRegistry:
        """Build the cluster registry directly from the unified pipeline configuration.

        Args:
            dataproc_cluster_settings (list[dict]): reference to the unified pipeline configuration.

        Returns:
            ClusterRegistry: the registry with clusters defined in the dataproc_cluster_settings


        """
        registry = cls()
        for cluster_settings in dataproc_cluster_settings:
            registry._add_cluster(cluster_settings)
        return registry
