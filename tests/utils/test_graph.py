from collections.abc import Set
from typing import Generic, TypeVar

import pytest
from attrs import define

from htap.utils.graph import DirectedGraph, UndirectedGraph

T = TypeVar("T")


@define
class GraphInput(Generic[T]):
    nodes: Set[T]
    edges: Set[tuple[T, T]]


@pytest.fixture(name="empty")
def fixture_empty() -> GraphInput[int]:
    return GraphInput(nodes=set(), edges=set())


@pytest.fixture(name="invalid")
def fixture_invalid() -> GraphInput[int]:
    return GraphInput(nodes={0}, edges={(0, 1)})


@pytest.fixture(name="single")
def fixture_single() -> GraphInput[int]:
    return GraphInput(nodes={0}, edges=set())


@pytest.fixture(name="chain")
def fixture_chain() -> GraphInput[int]:
    return GraphInput(nodes={0, 1, 2}, edges={(0, 1), (1, 2)})


@pytest.fixture(name="star")
def fixture_star() -> GraphInput[str]:
    return GraphInput(
        nodes={"center", "north", "south", "east", "west"},
        edges={
            ("center", "north"),
            ("center", "south"),
            ("center", "east"),
            ("center", "west"),
        },
    )


@pytest.fixture(name="ring")
def fixture_ring() -> GraphInput[str]:
    return GraphInput(
        nodes={"w", "a", "s", "d"},
        edges={("w", "a"), ("a", "s"), ("s", "d"), ("d", "w")},
    )


@pytest.fixture(name="double_ring")
def fixture_double_ring() -> GraphInput[str]:
    return GraphInput(
        nodes={"a", "b", "c"},
        edges={
            ("a", "b"),
            ("b", "a"),
            ("a", "c"),
            ("c", "a"),
            ("b", "c"),
            ("c", "b"),
        },
    )


@pytest.fixture(name="clique")
def fixture_clique() -> GraphInput[int]:
    return GraphInput(
        nodes={0, 1, 2, 3},
        edges={
            (0, 1),
            (0, 2),
            (0, 3),
            (1, 0),
            (1, 2),
            (1, 3),
            (2, 0),
            (2, 1),
            (2, 3),
            (3, 0),
            (3, 1),
            (3, 2),
        },
    )


class TestDirectedGraph:
    def test_empty(self, empty: GraphInput[int]) -> None:
        graph = DirectedGraph[int](nodes=empty.nodes, edges=empty.edges)

        assert graph.nodes == empty.nodes
        assert graph.edges == empty.edges

        with pytest.raises(ValueError):
            graph.get_neighbors(0)

    def test_invalid(self, invalid: GraphInput[int]) -> None:
        with pytest.raises(ValueError):
            DirectedGraph[int](nodes=invalid.nodes, edges=invalid.edges)

    def test_single(self, single: GraphInput[int]) -> None:
        graph = DirectedGraph[int](nodes=single.nodes, edges=single.edges)

        assert graph.nodes == single.nodes
        assert graph.edges == single.edges

        assert graph.get_neighbors(0) == set()

    def test_chain(self, chain: GraphInput[int]) -> None:
        graph = DirectedGraph[int](nodes=chain.nodes, edges=chain.edges)

        assert graph.nodes == chain.nodes
        assert graph.edges == chain.edges

        assert graph.get_neighbors(0) == {1}
        assert graph.get_neighbors(1) == {2}
        assert graph.get_neighbors(2) == set()

    def test_star(self, star: GraphInput[str]) -> None:
        graph = DirectedGraph[str](nodes=star.nodes, edges=star.edges)

        assert graph.nodes == star.nodes
        assert graph.edges == star.edges

        assert graph.get_neighbors("center") == {"north", "south", "east", "west"}
        assert graph.get_neighbors("north") == set()
        assert graph.get_neighbors("south") == set()
        assert graph.get_neighbors("east") == set()
        assert graph.get_neighbors("west") == set()

    def test_ring(self, ring: GraphInput[str]) -> None:
        graph = DirectedGraph[str](nodes=ring.nodes, edges=ring.edges)

        assert graph.nodes == ring.nodes
        assert graph.edges == ring.edges

        assert graph.get_neighbors("w") == {"a"}
        assert graph.get_neighbors("a") == {"s"}
        assert graph.get_neighbors("s") == {"d"}
        assert graph.get_neighbors("d") == {"w"}

    def test_double_ring(self, double_ring: GraphInput[str]) -> None:
        graph = DirectedGraph[str](nodes=double_ring.nodes, edges=double_ring.edges)

        assert graph.nodes == double_ring.nodes
        assert graph.edges == double_ring.edges

        assert graph.get_neighbors("a") == {"b", "c"}
        assert graph.get_neighbors("b") == {"a", "c"}
        assert graph.get_neighbors("c") == {"a", "b"}

    def test_clique(self, clique: GraphInput[int]) -> None:
        graph = DirectedGraph[int](nodes=clique.nodes, edges=clique.edges)

        assert graph.nodes == clique.nodes
        assert graph.edges == clique.edges

        for node in (0, 1, 2, 3):
            assert graph.get_neighbors(node) == {0, 1, 2, 3} - {node}

    def test_self_cycle(self) -> None:
        nodes = {"left", "right"}
        edges = {
            ("left", "left"),
            ("left", "right"),
            ("right", "left"),
            ("right", "right"),
        }

        graph = DirectedGraph[str](nodes=nodes, edges=edges)

        assert graph.nodes == nodes
        assert graph.edges == edges

        assert graph.get_neighbors("left") == {"left", "right"}
        assert graph.get_neighbors("right") == {"left", "right"}


def reverse_edges(edges: Set[tuple[T, T]]) -> Set[tuple[T, T]]:
    return {(to_node, from_node) for from_node, to_node in edges}


class TestUndirectedGraph:
    def test_empty(self, empty: GraphInput[int]) -> None:
        graph = UndirectedGraph[int](nodes=empty.nodes, edges=empty.edges)

        assert graph.nodes == empty.nodes
        assert graph.edges == empty.edges | reverse_edges(empty.edges)

        with pytest.raises(ValueError):
            graph.get_neighbors(0)

    def test_invalid(self, invalid: GraphInput[int]) -> None:
        with pytest.raises(ValueError):
            UndirectedGraph[int](nodes=invalid.nodes, edges=invalid.edges)

    def test_single(self, single: GraphInput[int]) -> None:
        graph = UndirectedGraph[int](nodes=single.nodes, edges=single.edges)

        assert graph.nodes == single.nodes
        assert graph.edges == single.edges | reverse_edges(single.edges)

        assert graph.get_neighbors(0) == set()

    def test_chain(self, chain: GraphInput[int]) -> None:
        graph = UndirectedGraph[int](nodes=chain.nodes, edges=chain.edges)

        assert graph.nodes == chain.nodes
        assert graph.edges == chain.edges | reverse_edges(chain.edges)

        assert graph.get_neighbors(0) == {1}
        assert graph.get_neighbors(1) == {0, 2}
        assert graph.get_neighbors(2) == {1}

    def test_star(self, star: GraphInput[str]) -> None:
        graph = UndirectedGraph[str](nodes=star.nodes, edges=star.edges)

        assert graph.nodes == star.nodes
        assert graph.edges == star.edges | reverse_edges(star.edges)

        assert graph.get_neighbors("center") == {"north", "south", "east", "west"}
        assert graph.get_neighbors("north") == {"center"}
        assert graph.get_neighbors("south") == {"center"}
        assert graph.get_neighbors("east") == {"center"}
        assert graph.get_neighbors("west") == {"center"}

    def test_ring(self, ring: GraphInput[str]) -> None:
        graph = UndirectedGraph[str](nodes=ring.nodes, edges=ring.edges)

        assert graph.nodes == ring.nodes
        assert graph.edges == ring.edges | reverse_edges(ring.edges)

        assert graph.get_neighbors("w") == {"a", "d"}
        assert graph.get_neighbors("a") == {"s", "w"}
        assert graph.get_neighbors("s") == {"d", "a"}
        assert graph.get_neighbors("d") == {"w", "s"}

    def test_double_ring(self, double_ring: GraphInput[str]) -> None:
        graph = UndirectedGraph[str](nodes=double_ring.nodes, edges=double_ring.edges)

        assert graph.nodes == double_ring.nodes
        assert graph.edges == double_ring.edges

        assert graph.get_neighbors("a") == {"b", "c"}
        assert graph.get_neighbors("b") == {"a", "c"}
        assert graph.get_neighbors("c") == {"a", "b"}

    def test_clique(self, clique: GraphInput[int]) -> None:
        graph = UndirectedGraph[int](nodes=clique.nodes, edges=clique.edges)

        assert graph.nodes == clique.nodes
        assert graph.edges == clique.edges | reverse_edges(clique.edges)

        for node in (0, 1, 2, 3):
            assert graph.get_neighbors(node) == {0, 1, 2, 3} - {node}

    def test_self_cycle(self) -> None:
        nodes = {"left", "right"}
        edges = {
            ("left", "left"),
            ("left", "right"),
            ("right", "right"),
        }

        graph = UndirectedGraph[str](nodes=nodes, edges=edges)

        assert graph.nodes == nodes
        assert graph.edges == edges | reverse_edges(edges)

        assert graph.get_neighbors("left") == {"left", "right"}
        assert graph.get_neighbors("right") == {"left", "right"}
