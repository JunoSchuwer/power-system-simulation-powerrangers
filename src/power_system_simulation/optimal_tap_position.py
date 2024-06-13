"""
optimal_tap_position.py

This module provides functionality to simulate and determine the optimal tap positions in a power
system network. The main goal is to achieve optimal power distribution by adjusting transformer taps.
"""

import pandas as pd
from power_grid_model.utils import json_deserialize, json_serialize_to_file
from power_system_simulation.pgm_calculation_module import pgm_calculation

class InvalidMode(Exception):
    """Exception raised for an invalid mode, mode should be either 0(voltage) or 1(losses)"""

def optimal_tap_pos(input_network_data: str, path_active_power_profile: str, path_reactive_power_profile: str, mode=0):
    """
        Determines the optimal tap position for transformers in a power system network.

    Args:
        input_network_data (str): Path to the network configuration data file in JSON format.
        path_active_power_profile (str): Path to the active power profile data file in Parquet format.
        path_reactive_power_profile (str): Path to the reactive power profile data file in Parquet format.
        mode (int, optional): Optimization mode; 0 for minimum voltage deviation, 1 for minimum losses. Default is 0.

    Returns:
        int: The optimal tap position.

    Raises:
        InvalidMode: If the mode is not 0 or 1.

    This function processes the input network data and power profiles, and uses the `pgm_calculation` function 
    to obtain data frames containing max and min voltages, and line losses. It then determines the optimal
    position by iterating through all possible tap positions, comparing voltage deviations or losses as per the mode
    """


    if mode not in [0, 1]:
        raise InvalidMode("Mode must either be 0 or 1")

    with open(input_network_data, encoding="utf-8") as ind:
        input_data = json_deserialize(ind.read())

    active_power_profile = pd.read_parquet(path_active_power_profile)
    reactive_power_profile = pd.read_parquet(path_reactive_power_profile)

    for tap_pos in range(input_data["transformer"]["tap_min"][0], input_data["transformer"]["tap_max"][0]+1):
        input_data["transformer"]["tap_min"][0] = tap_pos

        json_serialize_to_file(input_network_data, input_data)

        voltage_df, loading_df = pgm_calculation(input_data, active_power_profile, reactive_power_profile)

        if mode == 0:
            avg_deviation_max_v_node = ((voltage_df["Max_Voltage"] - 1).abs()).mean()
            avg_deviation_min_v_node = ((voltage_df["Min_Voltage"] - 1).abs()).mean()
            avg_voltage_deviation = (avg_deviation_max_v_node + avg_deviation_min_v_node) / 2
            if tap_pos == input_data["transformer"]["tap_min"][0]:
                is_lower = avg_voltage_deviation
                store_pos = tap_pos
            if avg_voltage_deviation < is_lower:
                is_lower = avg_voltage_deviation
                store_pos = tap_pos

        if mode == 1:
            total_losses = loading_df["Total_Loss"]
            if tap_pos == input_data["transformer"]["tap_min"][0]:
                is_lower = total_losses
                store_pos = tap_pos
            if total_losses < is_lower:
                is_lower = total_losses
                store_pos = tap_pos

    return store_pos

pth_input_network_data = "tests/data/small_network/input/input_network_data.json"
pth_active_profile = "tests/data/small_network/input/active_power_profile.parquet"
pth_reactive_profile = "tests/data/small_network/input/reactive_power_profile.parquet"

pos=optimal_tap_pos(pth_input_network_data,pth_active_profile,pth_reactive_profile)
print(pos)