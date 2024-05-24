import unittest
import numpy as np

from power_system_simulation.graph_processing import (
    EdgeAlreadyDisabledError,
    GraphCycleError,
    GraphNotFullyConnectedError,
    GraphProcessor,
    IDNotFoundError,
    IDNotUniqueError,
    InputLengthDoesNotMatchError,
    NegativeVertexIDError,
    VertexIDcontainsnoninterger,
)


class TestGraphProcessor(unittest.TestCase):
    def test_unique_vertex_id(self):
        vertex_ids = np.array([1, 2, 2])
        edge_ids = np.array([4, 5])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1
        with self.assertRaises(IDNotUniqueError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_unique_edge_id(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([4, 4])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1
        with self.assertRaises(IDNotUniqueError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_unique_vertex_id_vs_edge_id(self):
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
        edge_ids = np.array([4, 5])
        edge_vertex_id_pairs = np.array([[1, 2]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1
        with self.assertRaises(InputLengthDoesNotMatchError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_valid_vertex_ids_in_pairs(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([4, 5])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 4]])  # 4 is not a valid vertex id
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(IDNotFoundError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_valid_vertex_ids_in_pairs_2(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([4, 5])
        edge_vertex_id_pairs = np.array([[1, 2], [4, 3]])  # 4 is not a valid vertex id
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(IDNotFoundError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_edge_enabled_length(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([4, 5])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True])
        source_vertex_id = 1

        with self.assertRaises(InputLengthDoesNotMatchError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_valid_source_vertex_id(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([4, 5])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 4  # Not a valid vertex id

        with self.assertRaises(IDNotFoundError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_fully_connected_graph(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([4])
        edge_vertex_id_pairs = np.array([[1, 2]])
        edge_enabled = np.array([True])
        source_vertex_id = 1

        with self.assertRaises(GraphNotFullyConnectedError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_no_cycles(self):
        vertex_ids = np.array([1, 2, 3])
        edge_ids = np.array([4, 5, 6])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3], [3, 1]])
        edge_enabled = np.array([True, True, True])
        source_vertex_id = 1

        with self.assertRaises(GraphCycleError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)

    def test_find_downstream_vertices(self):
        vertex_ids = np.array([1, 2, 3, 4, 5])
        edge_ids = np.array([6, 7, 8, 9])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3], [3, 4], [5, 4]])
        edge_enabled = np.array([True, True, True, True])
        source_vertex_id = 1
        test_graph_find_downstream = GraphProcessor(
            vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id
        )
        with self.assertRaises(IDNotFoundError):
            test_graph_find_downstream.find_downstream_vertices(10)
        self.assertEqual(test_graph_find_downstream.find_downstream_vertices(7), [3, 4, 5])
        self.assertEqual(test_graph_find_downstream.find_downstream_vertices(9), [5])

    def test_find_alternative_edges(self):
        vertex_ids = np.array([0, 2, 4, 6, 10])
        edge_ids = np.array([1, 3, 5, 7, 8, 9])
        edge_vertex_id_pairs = np.array([[0, 2], [0, 4], [0, 6], [2, 4], [4, 6], [2, 10]])
        edge_enabled = np.array([True, True, True, False, False, True])
        source_vertex_id = 0
        test_graph_find_alternative = GraphProcessor(
            vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id
        )
        with self.assertRaises(IDNotFoundError):
            test_graph_find_alternative.find_alternative_edges(11)
        with self.assertRaises(EdgeAlreadyDisabledError):
            test_graph_find_alternative.find_alternative_edges(7)
        self.assertEqual(test_graph_find_alternative.find_alternative_edges(3), [7, 8])

    def test_find_alternative_edges_2(self):
        vertex_ids = np.array([0, 2, 4, 6, 10])
        edge_ids = np.array([1, 3, 5, 7, 8, 9])
        edge_vertex_id_pairs = np.array([[0, 2], [0, 4], [0, 6], [2, 4], [4, 6], [2, 10]])
        edge_enabled = np.array([True, True, False, False, True, True])
        source_vertex_id = 0
        test_graph_find_alternative_2 = GraphProcessor(
            vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id
        )
        self.assertEqual(test_graph_find_alternative_2.find_alternative_edges(8), [5])


if __name__ == "__main__":
    unittest.main()
