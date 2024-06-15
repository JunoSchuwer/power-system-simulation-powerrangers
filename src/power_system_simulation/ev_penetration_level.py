"""This module can be used to calculate grid voltages and loading using EV power profiles
"""

import json
import math
import random

import pandas as pd
from power_grid_model import CalculationType, initialize_array
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import assert_valid_batch_data

from power_system_simulation.graph_processing import GraphProcessor
from power_system_simulation.input_data_validity_check import reformat_pgm_to_array
from power_system_simulation.pgm_calculation_module import PGMcalculation


def ev_penetration_calculation(
    model_ev,
    model_ev_arrays,
    path_input_network_data: str,
    path_ev_power_profile: str,
    path_meta_data: str,
    penetration_level_percentage: int,
    assert_valid_pwr_profile=False,
):
    """
    Perform EV (Electric Vehicle) penetration calculation based on input network data,
    EV power profiles, metadata, and penetration level percentage.

    Parameters:
    - path_input_network_data (str): Path to the JSON file containing network data.
    - path_ev_power_profile (str): Path to the Parquet file containing EV power profiles.
    - path_meta_data (str): Path to the JSON file containing metadata.
    - penetration_level_percentage (int): Percentage of penetration level for EVs.

    Returns:
    - df_voltages (DataFrame): DataFrame containing aggregated voltage data.
    - df_line_loading (DataFrame): DataFrame containing aggregated line loading data.
    """

    # Create array model of network
    with open(path_input_network_data, encoding="utf-8") as ind:
        input_network = json_deserialize(ind.read())
    input_network_array_model = model_ev_arrays

    # Find list of LV feeders
    with open(path_meta_data, encoding="utf-8") as metadata:
        meta_data = json.load(metadata)
    lv_feeders_list = meta_data["lv_feeders"]

    # Find downstream nodes of LV feeders
    downstream_nodes_lv_feeders = []
    for lv_feeder_id in lv_feeders_list:
        downstream_nodes = input_network_array_model.find_downstream_vertices(lv_feeder_id)
        downstream_nodes_lv_feeders.append(downstream_nodes)

    # Create dict for quick finding sym_load ids
    sym_load_node_to_id_dict = {}
    for sym_load in input_network["sym_load"]:
        sym_load_node_to_id_dict[sym_load[1]] = sym_load[0]

    # Create list of downstream sym_loads of LV feeders
    downstream_sym_loads_lv_feeders = []
    for downstream_nodes in downstream_nodes_lv_feeders:
        temp_list = []
        for node in downstream_nodes:
            if node in sym_load_node_to_id_dict:
                temp_list.append(node)
        downstream_sym_loads_lv_feeders.append(temp_list)

    # Calculate number of EVs per feeder
    number_ev_per_feeder = math.floor(
        (penetration_level_percentage / 100) * len(sym_load_node_to_id_dict) / len(lv_feeders_list)
    )

    # Import EV power profiles
    ev_power_profiles = pd.read_parquet(path_ev_power_profile)
    timestamps_ev = ev_power_profiles.index

    # Select random profiles
    list_ev_profile_columns = list(range(0, ev_power_profiles.shape[1]))
    random_list_ev_profile_columns = random.sample(list_ev_profile_columns, number_ev_per_feeder * len(lv_feeders_list))

    # Select random sym_loads
    syms_load_ids_update = []
    for downstream_sym_loads in downstream_sym_loads_lv_feeders:
        selected_sym_loads = random.sample(downstream_sym_loads, number_ev_per_feeder)
        for sym_load_id in selected_sym_loads:
            syms_load_ids_update.append(sym_load_node_to_id_dict[sym_load_id])

    # Create update data
    sym_load_update_data = initialize_array(
        "update", "sym_load", (ev_power_profiles.shape[0], len(syms_load_ids_update))
    )
    sym_load_update_data["id"] = syms_load_ids_update
    sym_load_update_data["p_specified"] = ev_power_profiles.iloc[:, random_list_ev_profile_columns].values.reshape(
        -1, len(syms_load_ids_update)
    )
    update_data = {"sym_load": sym_load_update_data}

    # Validate batch data
    if not assert_valid_pwr_profile:
        assert_valid_batch_data(
            input_data=input_network, update_data=update_data, calculation_type=CalculationType.power_flow
        )

    # Run model and aggregate results
    model_ev.run_power_flow_calculation(update_data_calc=update_data, timestamps_given=timestamps_ev)
    df_voltages = model_ev.aggregate_voltages()
    df_line_loading = model_ev.aggregate_line_loading()

    return df_voltages, df_line_loading
