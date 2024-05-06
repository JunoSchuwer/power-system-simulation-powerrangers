from power_system_simulation.graph_processing import *

vertex_ids = [0, 1, 2, 3]
edge_ids = [0, 1, 2]
edge_vertex_id_pairs = [(0, 1), (1, 2), (2, 3)] #will look like: 0-1-2-3
edge_enabled = [True, True, True, True]
source_vertex_id = 0

graph = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)