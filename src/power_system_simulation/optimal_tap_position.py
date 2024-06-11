from power_grid_model._utils import json_deserialize, json_serialize_to_file
from pgm_calculation_module import *
import numpy as np

class invalidmode(Exception):
    pass

def optimal_tap_pos(
    input_network_data:str,
    path_active_power_profile:str,
    path_reactive_power_profile:str,
    mode=0
    ):

    if mode not in [0, 1]:
        raise invalidmode("Mode must either be 0 or 1")
    
    with open(input_network_data, encoding="utf-8") as ind:
        input_data = json_deserialize(ind.read())

    active_power_profile = pd.read_parquet(path_active_power_profile)
    reactive_power_profile = pd.read_parquet(path_reactive_power_profile)

    min_pos = input_data["transformer"]["tap_min"][0]
    max_pos = input_data["transformer"]["tap_max"][0]

    for pos in range(max_pos, min_pos):
        input_data["transformer"]["tap_min"][0]= pos
        max_min_voltage_df, max_min_line_loading_df=pgm_calculation_module(input_data,active_power_profile,reactive_power_profile)
        avg_deviation_max_v_node= ((max_min_voltage_df["Max_Voltage"]-1).abs()).mean()
        avg_deviation_min_v_node= ((max_min_voltage_df["Min_Voltage"]-1).abs()).mean()
    return 