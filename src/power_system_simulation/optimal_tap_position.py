from power_system_simulation.pgm_calculation_module import *
from power_grid_model.utils import json_deserialize, json_serialize_to_file
import pandas as pd

class invalidmode(Exception):
    pass 

def optimal_tap_pos(input_network_data: str, path_active_power_profile: str, path_reactive_power_profile: str, mode=0):

    if mode not in [0, 1]:
        raise invalidmode("Mode must either be 0 or 1")

    with open(input_network_data, encoding="utf-8") as ind:
        input_data = json_deserialize(ind.read())

    active_power_profile = pd.read_parquet(path_active_power_profile)
    reactive_power_profile = pd.read_parquet(path_reactive_power_profile)

    for tap_pos in range(input_data["transformer"]["tap_min"][0], input_data["transformer"]["tap_max"][0]):
        input_data["transformer"]["tap_min"][0] = tap_pos

        json_serialize_to_file(input_network_data, input_data)

        max_min_voltage_df, max_min_line_loading_df = pgm_calculation(input_data, active_power_profile, reactive_power_profile)

        if mode == 0:
            avg_deviation_max_v_node = ((max_min_voltage_df["Max_Voltage"] - 1).abs()).mean()
            avg_deviation_min_v_node = ((max_min_voltage_df["Min_Voltage"] - 1).abs()).mean()
            avg_voltage_deviation = (avg_deviation_max_v_node + avg_deviation_min_v_node) / 2
            if tap_pos == input_data["transformer"]["tap_min"][0]:
                is_lower = avg_voltage_deviation
                store_pos = tap_pos
            if avg_voltage_deviation < is_lower:
                is_lower = avg_voltage_deviation
                store_pos = tap_pos

        if mode == 1:
            total_losses = max_min_line_loading_df["Total_Loss"]
            if tap_pos == input_data["transformer"]["tap_min"][0]:
                is_lower = total_losses
                store_pos = tap_pos
            if total_losses < is_lower:
                is_lower = total_losses
                store_pos = tap_pos

    return store_pos
