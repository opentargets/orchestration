"""Task chains."""


from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, Self

from airflow.models.taskmixin import DAGNode


class ProtoChain(Protocol):
    """Protocol for chaining operators together in the staging pipeline."""

    node_id: str
    dependency_node_ids: list[str]

    @property
    def entry_node(self) -> DAGNode: ...

    @property
    def exit_node(self) -> DAGNode: ...

    @property
    def is_root(self) -> bool:
        """Check if this chain is a root chain, meaning it has no prerequisites."""
        return not self.dependency_node_ids

    def _set_upstream(self, other: Self) -> Self:
        """Set this chain to be downstream of another chain."""
        if self.is_root:
            raise ValueError("Cannot set upstream for a root chain.")
        self.entry_node.set_upstream(other.exit_node)
        return self

    def _set_downstream(self, other: Self) -> Self:
        """Set this chain to be upstream of another chain."""
        self.exit_node.set_downstream(other.entry_node)
        return self

    def __rshift__(self, other: Self) -> Self:
        """Set this chain to be upstream of another chain using the >> operator."""
        return self._set_downstream(other)

    def __lshift__(self, other: Self) -> Self:
        """Set this chain to be downstream of another chain using the << operator."""
        return self._set_upstream(other)

    @staticmethod
    def chain_dependencies(nodes: Mapping[str, ProtoChain]):
        """Chain the dependencies between the given nodes based on their dependency_node_ids."""
        if nodes:
            for name, node in nodes.items():
                for dep_node_id in node.dependency_node_ids:
                    dep_node = nodes.get(dep_node_id)
                    if dep_node is None:
                        raise ValueError(f"Dependency {dep_node_id} of {name} not found in nodes.")
                    node._set_upstream(dep_node)
