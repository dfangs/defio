from collections.abc import Hashable, Mapping, Set
from typing import Generic, TypeVar

from attrs import define
from immutables import Map

_T = TypeVar("_T", bound=Hashable)


@define(frozen=True)
class DirectedGraph(Generic[_T]):
    """General-purpose immutable directed graph."""

    _graph: Mapping[_T, Set[_T]]

    def __init__(self, nodes: Set[_T], edges: Set[tuple[_T, _T]]) -> None:
        """
        Initializes this graph from the given set of nodes and edges.

        Raises a `ValueError` if any of the given edges connects nodes
        there are not in `nodes`.
        """
        graph = {node: set[_T]() for node in nodes}

        for from_node, to_node in edges:
            if from_node not in graph or to_node not in graph:
                raise ValueError("Each edge's nodes must come from the given `nodes`")
            graph[from_node].add(to_node)

        # Preemptively freeze the adjacency sets
        frozen_graph = Map(
            {node: frozenset(adjacency_set) for node, adjacency_set in graph.items()}
        )
        object.__setattr__(self, "_graph", frozen_graph)

    @property
    def nodes(self) -> Set[_T]:
        """Returns all nodes in this graph."""
        return frozenset(self._graph.keys())

    @property
    def edges(self) -> Set[tuple[_T, _T]]:
        """Returns all edges in this graph."""
        return frozenset(
            (from_node, to_node)
            for from_node, adjacency_set in self._graph.items()
            for to_node in adjacency_set
        )

    def get_neighbors(self, node: _T) -> Set[_T]:
        """
        Returns all neighbors of the given node (including the node itself
        if self-cycle exists).

        Raises a `ValueError` if the given node doesn't exist in this graph.
        """
        try:
            return self._graph[node]
        except KeyError as exc:
            raise ValueError("Node does not exist") from exc


@define(frozen=True)
class UndirectedGraph(DirectedGraph[_T]):
    """General-purpose immutable undirected graph."""

    def __init__(self, nodes: Set[_T], edges: Set[tuple[_T, _T]]) -> None:
        # Note: It's fine if `edges` already have the reverse edges
        reverse_edges = {(to_node, from_node) for from_node, to_node in edges}
        super().__init__(nodes, edges | reverse_edges)
