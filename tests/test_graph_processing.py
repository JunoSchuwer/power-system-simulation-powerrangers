from power_system_simulation.graph_processing import __init__
import numpy as np
import networkx as nx
import unittest


class TestMyClass(unittest.TestCase):
    def test_unique_vertex_ids(self):
        vertex_ids = np.array([1, 2, 2])
        edge_ids = np.array([1, 2])
        edge_vertex_id_pairs = np.array([[1, 2], [2, 3]])
        edge_enabled = np.array([True, True])
        source_vertex_id = 1

        with self.assertRaises(IDNotUniqueError):
            GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
