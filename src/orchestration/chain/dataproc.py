"""Dataproc chain implementation."""

from __future__ import annotations

from airflow.models import DAGNode
from airflow.models.baseoperator import chain

from orchestration.chain import ProtoChain
from orchestration.operators.dataproc import CreateClusterOperator, DeleteClusterOperator, SubmitJobOperator


class DataprocChain(ProtoChain):
    def __init__(
        self,
        node_id: str,
        dependency_node_ids: list[str],
        *,
        create_op: CreateClusterOperator,
        submit_op: SubmitJobOperator,
        delete_op: DeleteClusterOperator,
    ):
        self.node_id = node_id
        self._create_op = create_op
        self._submit_op = submit_op
        self._delete_op = delete_op
        chain(self._create_op, self._submit_op, self._delete_op)
        self.dependency_node_ids = dependency_node_ids or []

    @property
    def entry_node(self) -> DAGNode:
        """Get the entry node for this cluster handler, which is the cluster creation operator."""
        return self._submit_op

    @property
    def exit_node(self) -> DAGNode:
        """Get the exit node for this cluster handler, which is the cluster deletion operator."""
        return self._submit_op

    def get_cluster_operators(self) -> tuple[str, CreateClusterOperator, DeleteClusterOperator]:
        """Get the cluster management operators, which are the cluster creation and deletion operators."""
        return self._create_op.cluster_name, self._create_op, self._delete_op
