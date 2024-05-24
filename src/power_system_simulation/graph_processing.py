"""
This is a skeleton for the graph processing assignment.

We define a graph processor class with some function skeletons.
"""

# pylint: disable=W0611
# pylint: disable=R0913
import random
import time
from typing import List, Tuple
from networkx.exception import NetworkXNoCycle
import networkx as nx
import numpy as np


# pylint: disable=C0115
class IDNotFoundError(Exception):
    pass


class VertexIDcontainsnoninterger(Exception):
    pass


class InputLengthDoesNotMatchError(Exception):
    pass


class IDNotUniqueError(Exception):
    pass


class GraphNotFullyConnectedError(Exception):
    pass


class GraphCycleError(Exception):
    pass


class EdgeAlreadyDisabledError(Exception):
    pass


class NegativeVertexIDError(Exception):
    pass


# pylint: enable=C0115
class GraphProcessor:
    """
    General documentation of this class.
    You need to describe the purpose of this class and the functions in it.
    We are using an undirected graph in the processor.
    """

    def __init__(
        self,
        vertex_ids: np.ndarray,  # Changed data types - all can be interpreted as 1D or 2D (for ID pairs) arrays
        edge_ids: np.ndarray,
        edge_vertex_id_pairs: np.ndarray,
        edge_enabled: np.ndarray,
        source_vertex_id: int,
    ) -> None:
        """
        Initialize a graph processor object with an undirected graph.
        Only the edges which are enabled are taken into account.
        Check if the input is valid and raise exceptions if not.
        The following conditions should be checked:
            1. vertex_ids and edge_ids should be unique. (IDNotUniqueError)
            2. edge_vertex_id_pairs should have the same length as edge_ids. (InputLengthDoesNotMatchError)
            3. edge_vertex_id_pairs should contain valid vertex ids. (IDNotFoundError)
            4. edge_enabled should have the same length as edge_ids. (InputLengthDoesNotMatchError)
            5. source_vertex_id should be a valid vertex id. (IDNotFoundError)
            6. The graph should be fully connected. (GraphNotFullyConnectedError)
            7. The graph should not contain cycles. (GraphCycleError)
        If one certain condition is not satisfied, the error in the parentheses should be raised.

        Args:
            vertex_ids: list of vertex ids
            edge_ids: liest of edge ids
            edge_vertex_id_pairs: list of tuples of two integer
                Each tuple is a vertex id pair of the edge.
            edge_enabled: list of bools indicating of an edge is enabled or not
            source_vertex_id: vertex id of the source in the graph
        """
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
                    incorrect_value=pair[0]
                else:
                    incorrect_value=pair[1]
                raise IDNotFoundError(
                    "Values in edge_vertex_id_pairs must be valid vertex IDs, value: " + str(incorrect_value) + " not found!"
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
        Given an edge id, return all the vertices which are in the downstream of the edge,
            with respect to the source vertex.
            Including the downstream vertex of the edge itself!

        Only enabled edges should be taken into account in the analysis.
        If the given edge_id is a disabled edge, it should return empty list.
        If the given edge_id does not exist, it should raise IDNotFoundError.


        For example, given the following graph (all edges enabled):

            vertex_0 (source) --edge_1-- vertex_2 --edge_3-- vertex_4

        Call find_downstream_vertices with edge_id=1 will return [2, 4]
        Call find_downstream_vertices with edge_id=3 will return [4]

        Args:
            edge_id: edge id to be searched

        Returns:
            A list of all downstream vertices.
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
        Given an enabled edge, do the following analysis:
            If the edge is going to be disabled,
                which (currently disabled) edge can be enabled to ensure
                that the graph is again fully connected and acyclic?
            Return a list of all alternative edges.
        If the disabled_edge_id is not a valid edge id, it should raise IDNotFoundError.
        If the disabled_edge_id is already disabled, it should raise EdgeAlreadyDisabledError.
        If there are no alternative to make the graph fully connected again, it should return empty list.


        For example, given the following graph:

        vertex_0 (source) --edge_1(enabled)-- vertex_2 --edge_9(enabled)-- vertex_10
                 |                               |
                 |                           edge_7(disabled)
                 |                               |
                 -----------edge_3(enabled)-- vertex_4
                 |                               |
                 |                           edge_8(disabled)
                 |                               |
                 -----------edge_5(enabled)-- vertex_6

        Call find_alternative_edges with disabled_edge_id=1 will return [7]
        Call find_alternative_edges with disabled_edge_id=3 will return [7, 8]
        Call find_alternative_edges with disabled_edge_id=5 will return [8]
        Call find_alternative_edges with disabled_edge_id=9 will return []

        Args:
            disabled_edge_id: edge id (which is currently enabled) to be disabled

        Returns:
            A list of alternative edge ids.
        """
        # handle errors:
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

#pylint: disable 105
"""
def find_two_random(max_number) -> Tuple[int]:
    num1=random.randint(0,max_number)
    num2=random.randint(0,max_number)
    while num1==num2:
        num2=random.randint(0,max_number)
    if num2 < num1:
        num1,num2=num2,num1
    return num1,num2

start_generate=time.time()
#set number of vertices and cross connecting (random) edges which are disabled
number_of_vertices=100000
number_of_random_edges=100000#never much higher than number of vertices!!!

#generate vertex ids, edge_ids, edge enabled and source vertex
vertex_ids=np.array([i for i in range(number_of_vertices)])
edge_ids=np.array([i+number_of_vertices for i in range(number_of_vertices-1+number_of_random_edges)])
edge_enabled=np.array([True] * (number_of_vertices-1) + [False] * number_of_random_edges)
source_vertex_id=0

#generate edge vertex id pairs
vertex_pos=0
size=1
prev_vertices=[0]
edge_vertex_id_pairs_list=[]
connected_dict={0:[]}
while True:
    vertex_pos+=size
    if vertex_pos>number_of_vertices-1:
        break
    size*=2
    if vertex_pos+size>number_of_vertices:
        size=number_of_vertices-vertex_pos
    new_vertices=vertex_ids[vertex_pos:vertex_pos+size]
    for idx,i in enumerate(new_vertices):
        connected_prev=prev_vertices[idx//2]
        edge_vertex_id_pairs_list.append([connected_prev,i])
        connected_dict[i]=[connected_prev]
        connected_dict[connected_prev].append(i)
    prev_vertices=new_vertices

for i in range(number_of_random_edges):
    vertex1,vertex2=find_two_random(number_of_vertices-1)
    while vertex2 in connected_dict[vertex1]:
        vertex1,vertex2=find_two_random(number_of_vertices-1)
    edge_vertex_id_pairs_list.append([vertex1,vertex2])
    connected_dict[vertex1].append(vertex2)
    connected_dict[vertex2].append(vertex1)

edge_vertex_id_pairs=np.array(edge_vertex_id_pairs_list)

finish_generate=time.time()

#vertex_ids = np.array([0, 2, 4, 6, 10])
#edge_ids = np.array([1, 3, 5, 7, 8, 9])
#edge_vertex_id_pairs = np.array([[0, 2], [0, 4], [0, 6], [2, 4], [4, 6], [2, 10]])
#edge_enabled = np.array([True, True, True,False,False,True])
#source_vertex_id = 0


#vertex_ids = np.array([0, 1, 2, 3, 4])
#edge_ids = np.array([5, 6, 7, 8])
#edge_vertex_id_pairs = np.array([[0, 1], [1, 2], [2, 3], [3, 4]]) #will look like: 0-1-2-3
#edge_enabled = np.array([True, True, True,False])
#source_vertex_id = 0

print("generated network")

init_start=time.time()
graph = GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)
init_finish=time.time()

print("initialized")

alt_edges_start=time.time()
alternative_edges=graph.find_alternative_edges(edge_ids[number_of_vertices//10])
alt_edges_finish=time.time()

print(alternative_edges)
print("amount of alternative edges= "+str(len(alternative_edges)))
print("init time= "+str(init_finish-init_start))
print("alt edges time= "+str(alt_edges_finish-alt_edges_start))
print("generate time= "+str(finish_generate-start_generate))
"""
