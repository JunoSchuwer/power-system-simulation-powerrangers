"""
This module defines a GraphProcessor class for handling and analyzing an undirected graph.

The GraphProcessor class is designed to initialize, manipulate, and analyze the structure of an undirected graph.
It ensures all vertices and edges are properly validated and provides functionality to find downstream vertices,
identify alternative edges, and manage parent-child relationships within the graph.

Exceptions are raised for invalid operations such as using non-existent IDs, attempting to disable an already disabled
edge, or encountering cycles and disconnected components within the graph.

Usage of this class requires initializing it with vertex IDs, edge IDs, edge vertex ID pairs, edge statuses
(enabled/disabled), and a source vertex ID.

Classes:
    IDNotFoundError: Raised when a specified ID does not exist.
    VertexIDcontainsnoninterger: Raised when a vertex ID is not an integer.
    InputLengthDoesNotMatchError: Raised when the lengths of edge-related inputs do not match.
    IDNotUniqueError: Raised when IDs are not unique.
    GraphNotFullyConnectedError: Raised when the graph is not fully connected.
    GraphCycleError: Raised when the graph contains cycles.
    EdgeAlreadyDisabledError: Raised when an edge is already disabled.
    NegativeVertexIDError: Raised when a vertex ID is negative.

Methods:
    __init__(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id):
        Initializes the GraphProcessor with the given graph data and performs validations.

    find_downstream_vertices(edge_id):
        Returns a list of vertices downstream of a specified edge with respect to the source vertex. 
        Raises IDNotFoundError if the edge ID is invalid.

    find_alternative_edges(disabled_edge_id):
        Returns a list of alternative edge IDs that can be enabled to maintain graph connectivity and acyclicity after disabling the specified edge.
        Raises IDNotFoundError if the edge ID is invalid, EdgeAlreadyDisabledError if the edge is already disabled.

    create_children_parent_dictonary(start_vertex_id):
        Performs a depth-first search to construct parent-child relationships starting from the source vertex.

    create_network_dict():
        Constructs a dictionary mapping each vertex to its connected vertices and another mapping to edge IDs for those connections.
"""

from typing import List, Tuple
import networkx as nx
import numpy as np

class IDNotFoundError(Exception):
    '''Exception raises when no id is found'''

class VertexIDcontainsnoninterger(Exception):
    '''Exception raises when an ID is invalid'''

class InputLengthDoesNotMatchError(Exception):
    '''Exception raises when the amount of edges does not match the edge id pairs'''

class IDNotUniqueError(Exception):
    '''Exception raises when an ID is invalid'''

class GraphNotFullyConnectedError(Exception):
    '''Exception raises when an vertex has no (disabled) connection'''

class GraphCycleError(Exception):
    '''Exception raises when the graph is cyclic'''

class EdgeAlreadyDisabledError(Exception):
    '''Exception raises when an edge is disabled'''
    pass


class NegativeVertexIDError(Exception):
    '''Exception raises when an ID is invalid'''

class GraphProcessor:
    """
        Initializes the GraphProcessor with vertex IDs, edge IDs, edge vertex pairs, edge statuses, and source vertex ID.
        
        Validates the input data to ensure all IDs are integers, unique, and correctly linked. Checks for graph connectivity 
        and acyclicity, raising appropriate exceptions if any conditions are violated.
        
        Args:
            vertex_ids: An array of vertex IDs.
            edge_ids: An array of edge IDs.
            edge_vertex_id_pairs: An array of pairs representing edges between vertices.
            edge_enabled: An array indicating the enabled status of each edge.
            source_vertex_id: The ID of the source vertex.

        Raises:
            NegativeVertexIDError: If any vertex ID is negative.
            VertexIDcontainsnoninterger: If vertex IDs are not integers.
            IDNotUniqueError: If vertex IDs or edge IDs are not unique.
            InputLengthDoesNotMatchError: If lengths of edge-related inputs do not match.
            IDNotFoundError: If any edge or vertex ID does not exist in the graph.
            GraphNotFullyConnectedError: If the graph is not fully connected.
            GraphCycleError: If the graph contains cycles.
        """

    def __init__(self,
        vertex_ids: np.ndarray,
        edge_ids: np.ndarray,
        edge_vertex_id_pairs: np.ndarray,
        edge_enabled: np.ndarray,
        source_vertex_id: int,
        ) -> None:
    
        self.vertex_ids = vertex_ids
        self.edge_ids = edge_ids
        self.edge_vertex_id_pairs = edge_vertex_id_pairs
        self.edge_enabled = edge_enabled
        self.source_vertex_id = source_vertex_id

        if np.any(vertex_ids < 0):
            raise NegativeVertexIDError("Vertex ID must be a positive interger!")

        if not issubclass(vertex_ids.dtype.type, np.integer):
            raise VertexIDcontainsnoninterger("Vertex ID must be real interger!")

        if any(np.isin(vertex_ids, edge_ids)):  # Checks that all elements from the two arrays are different
            for i in vertex_ids:
                if i in edge_ids:
                    raise IDNotUniqueError(
                        "Vertex ID matches with edge ID, value: " + str(i) + " both vertex and edge ID"
                    )

        if len(vertex_ids) != len(set(vertex_ids)):  # Checks vertex_ids are unique
            raise IDNotUniqueError("Vertex IDs must be unique!")

        if len(edge_ids) != len(set(edge_ids)):  # Checks edge_ids are unique
            raise IDNotUniqueError("Edge IDs must be unique!")

        if len(edge_ids) != len(edge_vertex_id_pairs):
            raise InputLengthDoesNotMatchError("Length of edge_ids does not match the length of edge_vertex_id_pairs!")

        for pair in edge_vertex_id_pairs:
            if pair[0] not in vertex_ids or pair[1] not in vertex_ids:
                if pair[0] not in vertex_ids:
                    incorrect_value = pair[0]
                else:
                    incorrect_value = pair[1]
                raise IDNotFoundError(
                    "Values in edge_vertex_id_pairs must be valid vertex IDs, value: "
                    + str(incorrect_value)
                    + " not found!"
                )

        if len(edge_enabled) != len(edge_ids):
            raise InputLengthDoesNotMatchError("Length of edge_enabled does not match the length of edge_ids!")

        if source_vertex_id not in vertex_ids:
            raise IDNotFoundError("Source vertex ID is not a valid vertex ID!")

        self.graph_cycles = nx.Graph()
        self.graph_connection = nx.Graph()

        for i, row in enumerate(self.edge_vertex_id_pairs):  # Using enumerate for clarity
            if self.edge_enabled[i]:
                self.graph_cycles.add_edge(row[0], row[1])
            self.graph_connection.add_edge(row[0], row[1])  # Adding all edges for connection check

        # Add all vertices to ensure they are included in the connectivity check
        self.graph_connection.add_nodes_from(self.vertex_ids)

        if not nx.is_connected(self.graph_connection):
            raise GraphNotFullyConnectedError("The graph is not fully connected!")

        
        try:
            nx.find_cycle(self.graph_cycles)
        except:
            pass
        else:
            raise GraphCycleError("The graph contains cycles!")
        

        # create children and parent dictionary used in other functions:
        self.network_dict = {}
        self.network_dict_edge_id = {}
        self.children_dict = {}
        self.parent_dict = {}
        self.create_children_parent_dictonary(self.source_vertex_id)

    def find_downstream_vertices(self, edge_id: int) -> List[int]:
        """
        Returns a list of vertices downstream of the specified edge with respect to the source vertex.
        
        Only considers enabled edges. Raises IDNotFoundError if the edge ID is invalid.
        
        Args:
            edge_id: The ID of the edge to analyze.

        Returns:
            A list of downstream vertex IDs.

        Raises:
            IDNotFoundError: If the edge ID is invalid.
        """
        if edge_id not in self.edge_ids:
            raise IDNotFoundError("Given edge ID is not a valid edge ID!")
        # find vertices connected to the edge with ID: edge_id
        index_edge_downstream = np.where(self.edge_ids == edge_id)[0][0]
        edge_start = self.edge_vertex_id_pairs[index_edge_downstream]

        # check which of the two is the "downstream vertex" by checking which is the parent of the other
        visited = []
        if edge_start[0] == self.source_vertex_id:
            que = [edge_start[1]]
        elif self.parent_dict[edge_start[0]] == edge_start[1]:
            que = [edge_start[0]]
        else:
            que = [edge_start[1]]

        # visit every downstream vertex by doing a depth first search of all the childeren of the start vertex
        while len(que) > 0:
            visit = que[0]
            que.pop(0)
            visited.append(visit)
            for i in self.children_dict[visit]:
                que.insert(0, i)
        return visited

    def find_alternative_edges(self, disabled_edge_id: int) -> List[int]:
        """
        Returns a list of alternative edge IDs that can be enabled to maintain graph connectivity and acyclicity
        after disabling the specified edge.
        
        Raises IDNotFoundError if the edge ID is invalid and EdgeAlreadyDisabledError if the edge is already disabled.
        
        Args:
            disabled_edge_id: The ID of the edge to be disabled.

        Returns:
            A list of alternative edge IDs or an empty list if no alternatives exist.

        Raises:
            IDNotFoundError: If the edge ID is invalid.
            EdgeAlreadyDisabledError: If the edge is already disabled.
        """
        if disabled_edge_id not in self.edge_ids:
            raise IDNotFoundError("Edge ID: " + str(disabled_edge_id) + " trying to be disabled does not exist!")
        index_edge = np.where(self.edge_ids == disabled_edge_id)[0][0]
        if not self.edge_enabled[index_edge]:
            raise EdgeAlreadyDisabledError("Edge ID: " + str(disabled_edge_id) + " already disabled!")

        # find downsteam vertices of disabled edge
        downstream_list = self.find_downstream_vertices(disabled_edge_id)

        # check for each of the downsteam vertices whether there is a connected vertex which is not in
        # the list of downstream vertices and also connected via a disabled edge (vertex is negative in
        # self.network_dict[vertex])
        alt_edges = []
        for i in downstream_list:
            for neighbour in self.network_dict[i]:
                if neighbour is None:
                    pass
                elif neighbour >= 0 or abs(neighbour) in downstream_list:
                    continue
                index_edge_id_dict = self.network_dict[i].index(neighbour)
                alt_edges.append(self.network_dict_edge_id[i][index_edge_id_dict])

        return alt_edges

    def create_children_parent_dictonary(self, start_vertex_id: int) -> None:
        """
        This function does a depth first search, while creating
        a dictionary of parent and childeren of a certain vertex.
        ONLY enabled edges are considered.

        self.parent[vertex_ID] -> returns parent of vertex_ID (integrer)
        self.childeren[vertex_ID] -> returns LIST of childeren
        """
        self.create_network_dict()
        que = [start_vertex_id]
        self.children_dict = {}
        self.parent_dict[start_vertex_id] = None
        while len(que) > 0:
            visiting = que[0]
            que.pop(0)
            connected_visit = self.network_dict[visiting]
            children_visit = []
            for i in connected_visit:
                if i is None:
                    continue
                if i >= 0:
                    if i not in self.parent_dict:
                        self.parent_dict[i] = visiting
                        que.insert(0, i)
                        children_visit.append(i)
            self.children_dict[visiting] = children_visit

    def create_network_dict(self) -> None:
        """
        creates dictionary where self.network_dict[vertex_id] returns
        all vertices which are connected to this vertex. If the vertex ID
        is negative, it indicates that that connection edge is disabled.

        if vertex ID=0 -> instead of negative 0, value: "None" is used.

        e.g.: self.network_dict[x] -> returns -> [list of all connected vertices to vertex x]

        also, this function creates a dictionary which tells what the edge ID is of each of the connections which
        are listed in self.network_dict. So if for example vertex x is connected with: [1,2,3,4], than
        self.network_dict_edge_id[x] will return [5,6,7,8] where 5 is the edge ID of connection [x,1] and 6 the
        edge ID of connection [x,2].

        e.g.: self.network_dict_edge_id[x] -> returns -> [list of edge IDs of connections in self.network_dict[x]]
        """
        for idx_tup_enable, tup in enumerate(self.edge_vertex_id_pairs):
            for tup_idx in range(2):
                if tup[tup_idx] not in self.network_dict:
                    self.network_dict[tup[tup_idx]] = []
                    self.network_dict_edge_id[tup[tup_idx]] = []
                if tup[tup_idx - 1] not in self.network_dict[tup[tup_idx]]:
                    if self.edge_enabled[idx_tup_enable]:
                        self.network_dict[tup[tup_idx]].append(tup[tup_idx - 1])
                    else:
                        if tup[tup_idx - 1] == 0:
                            self.network_dict[tup[tup_idx]].append(None)
                        else:
                            self.network_dict[tup[tup_idx]].append(-tup[tup_idx - 1])
                    self.network_dict_edge_id[tup[tup_idx]].append(self.edge_ids[idx_tup_enable])
