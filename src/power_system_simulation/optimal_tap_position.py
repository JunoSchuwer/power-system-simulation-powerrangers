import pandas as pd
from power_grid_model.utils import json_deserialize, json_serialize_to_file
from power_system_simulation.pgm_calculation_module import *

class InvalidMode(Exception):
    """Exception raised for an invalid mode, mode should be either 0(voltage) or 1(losses)"""

def optimal_tap_pos(input_network_data: str, path_active_power_profile: str, path_reactive_power_profile: str, mode=0):
    '''Function takes 3 inputs: a network, power profiles, and a mode. The mode should either be 0 or 1 where 0 is 
    minimum voltage differentiation and 1 is minimum losses. The function processes the inputs, then uses 
    pgm_calculation_module to get data frames containing max, min voltage, and line losses. This is then processed 
    to find an optimum tap by running the function for every possible tap position, which is then compared to a 
    previous value.'''

    if mode not in [0, 1]:
        raise InvalidMode("Mode must either be 0 or 1")

    with open(input_network_data, encoding="utf-8") as ind:
        input_data = json_deserialize(ind.read())

    active_power_profile = pd.read_parquet(path_active_power_profile)
    reactive_power_profile = pd.read_parquet(path_reactive_power_profile)

    for tap_pos in range(input_data["transformer"]["tap_min"][0], input_data["transformer"]["tap_max"][0]):
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