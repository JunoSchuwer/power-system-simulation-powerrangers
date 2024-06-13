"""
optimal_tap_position.py

This module provides functionality to simulate and determine the optimal tap positions in a power
system network. The main goal is to achieve optimal power distribution by adjusting transformer taps.
"""

import pandas as pd
import numpy as np
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
    min_pos=np.take(input_data["transformer"]["tap_min"],0)
    max_pos=np.take(input_data["transformer"]["tap_max"],0)

    if min_pos>max_pos:
        temp_min_pos=max_pos
        max_pos=min_pos
        min_pos=temp_min_pos
    

    for tap_pos in range(min_pos, max_pos+1):
        input_data["transformer"]["tap_pos"][0] = tap_pos

        json_serialize_to_file(input_network_data, input_data)

        voltage_df, loading_df = pgm_calculation(input_network_data, path_active_power_profile, path_reactive_power_profile)

        if mode == 0:
            avg_voltage_deviation = ((voltage_df[["Max_Voltage", "Min_Voltage"]] - 1).mean(axis=1)).mean()

            if tap_pos == min_pos:
                min_deviation_value = avg_voltage_deviation
                optimal_tap_pos_value = tap_pos
            elif avg_voltage_deviation < min_deviation_value:
                min_deviation_value = avg_voltage_deviation
                optimal_tap_pos_value = tap_pos

        elif mode == 1:
            total_losses_tap = sum(loading_df["Total_Loss"])
            if tap_pos==min_pos:
                total_losses_min= total_losses_tap
                optimal_tap_pos_value = tap_pos
            elif total_losses_tap < total_losses_min:
                total_losses_min = total_losses_tap
                optimal_tap_pos_value = tap_pos

    return optimal_tap_pos_value

pth_input_network_data = "tests/data/small_network/input/input_network_data.json"
pth_active_profile = "tests/data/small_network/input/active_power_profile.parquet"
pth_reactive_profile = "tests/data/small_network/input/reactive_power_profile.parquet"

pos=optimal_tap_pos(pth_input_network_data,pth_active_profile,pth_reactive_profile,0)
print(pos)