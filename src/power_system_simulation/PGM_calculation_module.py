# Assignment 2: Power Grid Model
from typing import Dict

import numpy as np
import pandas as pd
import power_grid_model as pgm
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import assert_valid_batch_data, assert_valid_input_data


class ProfileNotMatchingError(Exception):
    """Error raised when active and reactive load profiles do not have matching timestamps and/or load ids"""


def PGM_calculation(input_network_data: Dict, path_active_power_profile: str, path_reactive_power_profile: str):

    # deserialize json file of network
    with open(input_network_data) as ind:
        input_data = json_deserialize(ind.read())

    # load parquet files of active and reactive power
    active_power_profile = pd.read_parquet(path_active_power_profile)
    reactive_power_profile = pd.read_parquet(path_reactive_power_profile)
    print(reactive_power_profile)
    # validate input data, raises `ValidationException`
    assert_valid_input_data(input_data=input_data, calculation_type=CalculationType.power_flow)

    # construct model
    model = PowerGridModel(input_data=input_data)

    # check if IDs and timestamps match
    if (active_power_profile.columns != reactive_power_profile.columns).any():
        raise ProfileNotMatchingError("Load IDs of active and reactive power do not match!")
    if not active_power_profile.index.equals(reactive_power_profile.index):
        raise ProfileNotMatchingError("Timestamps of active and reactive power do not match!")

    # Create a PGM batch update dataset with the active and reactive load profiles
    batch_update_dataset = initialize_array("update", "sym_load", active_power_profile.shape)
    batch_update_dataset["id"] = active_power_profile.columns.to_numpy()
    batch_update_dataset["p_specified"] = active_power_profile.to_numpy()
    batch_update_dataset["q_specified"] = reactive_power_profile.to_numpy()
    update_data = {"sym_load": batch_update_dataset}

    # validate batch update data, raises `ValidationException`
    assert_valid_batch_data(input_data=input_data, update_data=update_data, calculation_type=CalculationType.power_flow)

    # calculate output data
    output_data = model.calculate_power_flow(
        update_data=update_data, calculation_method=CalculationMethod.newton_raphson
    )

    u_pu, node_id, line_loading, line_id, p_from, p_to = (
        output_data["node"]["u_pu"],
        output_data["node"]["id"],
        output_data["line"]["loading"],
        output_data["line"]["id"],
        output_data["line"]["p_from"],
        output_data["line"]["p_to"],
    )
    timestamps = active_power_profile.index

    # Aggregate the power flow: each row representing a timestamp, with max and min p.u. voltage + node IDs
    max_min_voltages = []
    for timestamp_idx, timestamp in enumerate(timestamps):
        u_pu_max = u_pu[timestamp_idx].max()
        u_pu_max_node_id = node_id[timestamp_idx, np.argmax(u_pu[timestamp_idx])]
        u_pu_min = u_pu[timestamp_idx].min()
        u_pu_min_node_id = node_id[timestamp_idx, np.argmin(u_pu[timestamp_idx])]
        max_min_voltages.append(
            [
                timestamp,
                u_pu_max,
                u_pu_max_node_id,
                u_pu_min,
                u_pu_min_node_id,
            ]
        )

    # Aggregate the power flow: each row representing a line, with:
    # max and min loading in p.u. of the specific line across all timeline + Energy losses in KWh
    max_min_line_loading = []
    for id_idx, id in enumerate(np.unique(line_id)):
        loads = line_loading[line_id == id]
        load_max, time_load_max, load_min, time_load_min = (
            loads.max(),
            timestamps[loads.argmax()],
            loads.min(),
            timestamps[loads.argmin()],
        )
        e_losses = abs(p_from[:, id_idx] + p_to[:, id_idx])  # p_from and p_to are in Watts
        e_losses_kwh = np.trapz(e_losses) / 1000
        max_min_line_loading.append([id, e_losses_kwh, load_max, time_load_max, load_min, time_load_min])

    # convert from list of lists to pandas dataframe (table)
    max_min_voltage_df_columns = [
        "Timestamp",
        "Max_Voltage",
        "Max_Voltage_Node",
        "Min_Voltage",
        "Min_Voltage_Node",
    ]
    max_min_voltage_df = pd.DataFrame(max_min_voltages, columns=max_min_voltage_df_columns)
    max_min_voltage_df.set_index("Timestamp", inplace=True)

    max_min_line_loading_df_columns = [
        "Line_ID",
        "Total_Loss",
        "Max_Loading",
        "Max_Loading_Timestamp",
        "Min_Loading",
        "Min_Loading_Timestamp",
    ]
    max_min_line_loading_df = pd.DataFrame(max_min_line_loading, columns=max_min_line_loading_df_columns)
    max_min_line_loading_df.set_index("Line_ID", inplace=True)

    return max_min_voltage_df, max_min_line_loading_df


"""
#run some tests with test data
input_network_data = "tests/data/input/input_network_data.json"
path_active_profile = "tests/data/input/active_power_profile.parquet"
path_reactive_profile = "tests/data/input/reactive_power_profile.parquet"
max_min_voltages, max_min_line_loading=PGM_calculation(input_network_data, path_active_profile, path_reactive_profile)

print(max_min_voltages)
print(max_min_line_loading)

#to test load IDs not matching:
#active_power_profile = active_power_profile.rename(columns={8:222})

#to test timestamps not matching:
#active_power_profile.rename(index={pd.Timestamp("2024-01-01 00:00:00"):pd.Timestamp("2000-01-01 00:00:00")}, inplace=True)
"""
