import json

import numpy as np
import pandas as pd
import pytest
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import ValidationException, assert_valid_batch_data, assert_valid_input_data

from power_system_simulation.graph_processing import *
from power_system_simulation.input_data_validity_check import *
from power_system_simulation.pgm_calculation_module import (
    ProfileLoadIDsNotMatchingError,
    ProfileTimestampsNotMatchingError
)


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
    all_node_ids = [i[0] for i in input_network["node"]]

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


def test_input_validity_check_function():
    Errors = [
        ValidationException,
        MultipleTransformersError,
        MultipleSourcesError,
        InvalidFeederIDError,
        LVfeederNotMatchTransformOutError,
        GraphNotFullyConnectedError,
        GraphCycleError,
        ProfileTimestampsNotMatchingError,
        ProfileTimestampsNotMatchingError,
        ProfileLoadIDsNotMatchingError,
        LoadProfileIDsNotSymLoadError,
        InsufficientEVchargingProfilesError,
    ]

    for i in range(1, 13):
        pth_input_network_data = "tests/data/small_network/input/input_network_data.json"
        pth_active_profile = "tests/data/small_network/input/active_power_profile.parquet"
        pth_reactive_profile = "tests/data/small_network/input/reactive_power_profile.parquet"
        pth_ev_power_profile = "tests/data/small_network/input/ev_active_power_profile.parquet"
        pth_meta_data = "tests/data/small_network/input/meta_data.json"
        with pytest.raises(Errors[i - 1]):
            validate_input_data(
                pth_input_network_data,
                pth_active_profile,
                pth_reactive_profile,
                pth_ev_power_profile,
                pth_meta_data,
                test_case=i,
            )
