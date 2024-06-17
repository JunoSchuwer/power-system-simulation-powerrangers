"""
This function checks whether the input data is valid. It checks for:

The LV grid should be a valid PGM input data.
The LV grid has exactly one transformer, and one source.
All IDs in the LV Feeder IDs are valid line IDs.
All the lines in the LV Feeder IDs have the from_node the same as the to_node of the transformer.
The grid is fully connected in the initial state.
The grid has no cycles in the initial state.
The timestamps are matching between the active load profile, reactive load profile, and EV charging profile.
The IDs in active load profile and reactive load profile are matching.
The IDs in active load profile and reactive load profile are valid IDs of sym_load.
The number of EV charging profile is at least the same as the number of sym_load.

"""

import json

import numpy as np
import pandas as pd
from power_grid_model import CalculationType
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import assert_valid_input_data

from power_system_simulation.graph_processing import GraphProcessor
from power_system_simulation.pgm_calculation_module import (
    ProfileLoadIDsNotMatchingError,
    ProfileTimestampsNotMatchingError,
)


class MultipleTransformersError(Exception):
    """Error raised when LV grid has multiple transformers"""


class MultipleSourcesError(Exception):
    """Error raised when LV grid has multiple sources"""


class InvalidFeederIDError(Exception):
    """Error raised when LV Feeder IDs are not all valid line IDs"""


class LVfeederNotMatchTransformOutError(Exception):
    """Error raised when LV feeder IDs do not have the same from_node as the to_node of the transformer"""


class LoadProfileIDsNotSymLoadError(Exception):
    """Error raised when active or reactive load profile are not valid IDs of sym loads"""


class InsufficientEVchargingProfilesError(Exception):
    """Error raised when the number of EV charging profiles is smaller than the numer of sym loads"""


def validate_input_data(
    path_input_network_data: str,
    path_active_power_profile=0,
    path_reactive_power_profile=0,
    path_ev_active_power_profile=0,
    path_meta_data=0,
    test_case=0,
):
    """Input data validity check:
    Check the following validity criteria for the input data. Raise or passthrough relevant errors.

    The LV grid should be a valid PGM input data.
    The LV grid has exactly one transformer, and one source.
    All IDs in the LV Feeder IDs are valid line IDs.
    All the lines in the LV Feeder IDs have the from_node the same as the to_node of the transformer.
    The grid is fully connected in the initial state.
    The grid has no cycles in the initial state.
    The timestamps are matching between the active load profile, reactive load profile, and EV charging profile.
    The IDs in active load profile and reactive load profile are matching.
    The IDs in active load profile and reactive load profile are valid IDs of sym_load.
    The number of EV charging profile is at least the same as the number of sym_load.

    Args:
        path_input_network_data
        path_active_power_profile
        path_reactive_power_profile
        path_ev_active_power_profile
        path_meta_data
        test_case: see test file test_input_validity_check.py. Defaults to 0, which means no testing

    Raises:
        ValidationException
        MultipleTransformersError
        MultipleSourcesError
        InvalidFeederIDError
        LVfeederNotMatchTransformOutError
        ProfileTimestampsNotMatchingError
        ProfileTimestampsNotMatchingError
        ProfileLoadIDsNotMatchingError
        LoadProfileIDsNotSymLoadError
        InsufficientEVchargingProfilesError
    """
    # deserialize json file of network
    with open(path_input_network_data, encoding="utf-8") as ind:
        input_network = json_deserialize(ind.read())

    if path_meta_data != 0:
        # deserialize json file of meta_data.json
        with open(path_meta_data, encoding="utf-8") as metadata:
            meta_data = json.load(metadata)

    # load parquet files of active and reactive power
    if path_active_power_profile != 0:
        active_power_profile = pd.read_parquet(path_active_power_profile)
    if path_reactive_power_profile != 0:
        reactive_power_profile = pd.read_parquet(path_reactive_power_profile)

    # load parquet files of ev active power profile
    if path_ev_active_power_profile != 0:
        ev_power_profile = pd.read_parquet(path_ev_active_power_profile)

    # change data, for testing only, if ALL data types are provided
    if test_case != 0:
        if change_data_for_test:
            input_network, active_power_profile, reactive_power_profile, ev_power_profile, meta_data = (
                change_data_for_test(
                    input_network, active_power_profile, reactive_power_profile, ev_power_profile, meta_data, test_case
                )
            )

    # check for: LV grid is valid PGM input data
    assert_valid_input_data(input_data=input_network, calculation_type=CalculationType.power_flow)

    # check for: LV grid has only one transformer
    if len(input_network["transformer"]) != 1:
        raise MultipleTransformersError(
            "LV grid does not have exactly one transfromer but instead has: " + str(len(input_network["transformer"]))
        )

    # check for: LV grid has only one source
    if len(input_network["source"]) != 1:
        raise MultipleSourcesError(
            "LV grid does not have exactly one transfromer but instead has: " + str(len(input_network["source"]))
        )

    if path_meta_data != 0:
        # check for: lines in the LV Feeder IDs have the from_node the same as the to_node of the transformer
        to_node_transformer = input_network["transformer"][0][2]
        for i in meta_data["lv_feeders"]:
            for n in input_network["line"]:
                if n[0] == i:
                    if n[1] != to_node_transformer:
                        raise LVfeederNotMatchTransformOutError(
                            "Lines in the LV Feeder IDs don't have the same from_node as the to_node of the transformer"
                        )

        # check for: LV feeder IDs are valid line IDs
        input_network_line_ids = [i[0] for i in input_network["line"]]
        for i in meta_data["lv_feeders"]:
            if i not in input_network_line_ids:
                raise InvalidFeederIDError("LV feeder IDs are not valid line IDs")

    if 0 not in (path_active_power_profile, path_reactive_power_profile):
        # check for matching timestamps in load profiles:
        if not active_power_profile.index.equals(reactive_power_profile.index):
            raise ProfileTimestampsNotMatchingError("Timestamps of active and reactive power profile do not match!")

        # check for: matching IDs in active and reactive load profile:
        if (active_power_profile.columns != reactive_power_profile.columns).any():
            raise ProfileLoadIDsNotMatchingError("Load IDs of active and reactive power do not match!")

        # check for: The IDs in active load profile and reactive load profile are valid IDs of sym_load
        # Only check active load profile since it has already been checked that active and reactive have same IDs
        sym_load_ids = [i[0] for i in input_network["sym_load"]]
        for i in active_power_profile.columns:
            if i not in sym_load_ids:
                raise LoadProfileIDsNotSymLoadError(
                    "The IDs in active load profile and reactive load profile are not valid IDs of sym_load"
                )

    if 0 not in (path_active_power_profile, path_ev_active_power_profile):
        # check for matching timestamps in load profiles:
        if not active_power_profile.index.equals(ev_power_profile.index):
            raise ProfileTimestampsNotMatchingError("Timestamps of active and EV power profile do not match!")

    if path_ev_active_power_profile != 0:
        # check for: The number of EV charging profile is at least the same as the number of sym_load
        if (ev_power_profile.shape[1]) < len(input_network["sym_load"]):
            raise InsufficientEVchargingProfilesError(
                "The number of EV charging profile is lower than the number of sym_load"
            )

    # check for fully connected + no cycles using graph_processing.py:
    vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id = reformat_pgm_to_array(input_network)
    GraphProcessor(vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id)


def reformat_pgm_to_array(input_network_data):
    """
    change pgm format style to array format used in 'graph_processing.py', used for checking input data,
    finding downstream vertices and finding alternative edges.

    returns: vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id (see graph_processing)
    """
    vertex_ids = [i[0] for i in input_network_data["node"]]
    edge_ids = [input_network_data["transformer"][0][0]]
    edge_vertex_id_pairs = [[input_network_data["transformer"][0][1], input_network_data["transformer"][0][2]]]
    edge_enabled = [True]
    for i in input_network_data["line"]:
        edge_ids.append(i[0])
        edge_vertex_id_pairs.append([i[1], i[2]])
        if i[3] == 0 or i[4] == 0:
            edge_enabled.append(False)
        else:
            edge_enabled.append(True)

    source_vertex_id = input_network_data["source"][0][1]

    return (
        np.array(vertex_ids),
        np.array(edge_ids),
        np.array(edge_vertex_id_pairs),
        np.array(edge_enabled),
        source_vertex_id,
    )


# changing data for datasets:
def change_data_for_test(
    input_network, active_power_profile, reactive_power_profile, ev_power_profile, meta_data, test_case
):
    """Function to change data to perform test cases:
    0: No test case
    1: LV grid is not valied PGM input data
    2: LV grid has more than one transformer
    3: LV grid has more than one source
    4: IDs in LV feeder IDs are not valid line IDs
    5: Lines in LV feeder IDs do not have the same from_node as the to_node of the transformer
    6: The grid is not fully connected in intial state
    7: The grid is cyclic in intial state
    8: Timestamps between active and reactive profile do not match
    9: Timestamps between active and EV profile do not match
    10: IDs in active and reactive profile do not match
    11: IDs in active load (and reactive) load profile are valid sym_load IDs
    12: the number of EV charging profile is not the same as or higher than the number of sym_load
    """
    all_ids = []
    for i in input_network:
        for n in input_network[i]:
            all_ids.append(n[0])
    max_id = max(all_ids)

    if test_case == 1:
        input_network["node"][0][0] = input_network["node"][1][0]
    if test_case == 2:
        input_network["transformer"] = np.append(input_network["transformer"], input_network["transformer"][0])
        input_network["transformer"][1][0] = max_id + 1
    if test_case == 3:
        input_network["source"] = np.append(input_network["source"], input_network["source"][0])
        input_network["source"][1][0] = max_id + 1
    if test_case == 4:
        meta_data["lv_feeders"][0] = max_id + 1
    if test_case == 5:
        to_node_transformer_false = input_network["transformer"][0][2] + 1
        for i in input_network["line"]:
            if i[0] == meta_data["lv_feeders"][0]:
                if to_node_transformer_false == i[2]:
                    to_node_transformer_false = to_node_transformer_false + 1
                i[1] = to_node_transformer_false
                break
    if test_case == 6:
        for i in meta_data["lv_feeders"]:
            for n in input_network["line"]:
                if n[0] == i:
                    n[4] = 0
    if test_case == 7:
        for i in input_network["line"]:
            i[3] = 1
            i[4] = 1
    if test_case == 8:
        reactive_power_profile.rename(
            index={pd.Timestamp(reactive_power_profile.index[0]): pd.Timestamp("2000-01-01 00:00:00")}, inplace=True
        )
    if test_case == 9:
        ev_power_profile.rename(
            index={pd.Timestamp(ev_power_profile.index[0]): pd.Timestamp("2000-01-01 00:00:00")}, inplace=True
        )
    if test_case == 10:
        act_columns = active_power_profile.columns
        active_power_profile = active_power_profile.rename(columns={act_columns[0]: (max(act_columns) + 1)})
    if test_case == 11:
        sym_load_id_wrong = max_id + 1
        act_columns = active_power_profile.columns
        active_power_profile = active_power_profile.rename(columns={act_columns[0]: sym_load_id_wrong})
        reactive_power_profile = reactive_power_profile.rename(columns={act_columns[0]: sym_load_id_wrong})
    if test_case == 12:
        ev_power_profile = ev_power_profile.drop(ev_power_profile.columns[-1], axis=1)

    return input_network, active_power_profile, reactive_power_profile, ev_power_profile, meta_data
