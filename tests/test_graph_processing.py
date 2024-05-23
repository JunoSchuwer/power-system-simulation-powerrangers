from power_system_simulation.graph_processing import (
    GraphProcessor,
    IDNotFoundError,
    VertexIDcontainsnoninterger,
    InputLengthDoesNotMatchError,
    IDNotUniqueError,
    GraphNotFullyConnectedError,
    GraphCycleError,
    NegativeVertexIDError,
)

import numpy as np
import networkx as nx
import unittest


class TestGraphProcessor(unittest.TestCase):
    def test_unique_vertex_id(self):
        vertex_ids = np.array([1, 2, 2])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(IDNotUniqueError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_negative_vertex_id(self):
        vertex_ids = np.array([1, -2, 3])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(NegativeVertexIDError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_non_innteger_vertex_id(self):
        vertex_ids = np.array([1, 2.2, 3])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(VertexIDcontainsnoninterger):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_unique_edge_ids(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([1, 1])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(IDNotUniqueError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_edge_vertex_id_pairs_length(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1
# "doesnt return expected error for some reason returns idnotunique"
        with self.assertRaises(InputLengthDoesNotMatchError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_valid_vertex_ids_in_pairs(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 4]])  # 4 is not a valid vertex id
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(IDNotFoundError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_edge_enabled_length(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True])
        source_vertex_id = 1

        with self.assertRaises(InputLengthDoesNotMatchError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_valid_source_vertex_id(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 4  # Not a valid vertex id

        with self.assertRaises(IDNotFoundError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_fully_connected_graph(self):
        vertex_ids = np.array([1, 2, 3, 4])
        edge_ids = np.array([1, 2, 3])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(GraphNotFullyConnectedError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
            
def test_no_cycles(self):
    vertex_ids = np.array([1, 2, 3])
    edge_ids = np.array([1, 2, 3])
    edge_vertex_id_pairs = np.array([[1, 2], [2, 3], [3, 1]])
    edge_enabled = np.array([True, True, True])
    source_vertex_id = 1

    with self.assertRaises(GraphCycleError):
        GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)


if __name__ == "__main__":
    unittest.main()