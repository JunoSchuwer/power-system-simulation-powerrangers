"""
optimal_tap_position.py

This module provides functionality to simulate and determine the optimal tap positions in a power
system network. The main goal is to achieve optimal power distribution by adjusting transformer taps.
"""

import numpy as np
from power_grid_model import initialize_array
from power_grid_model.utils import json_deserialize

from power_system_simulation.pgm_calculation_module import PGMcalculation


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

    min_pos = np.take(input_data["transformer"]["tap_min"], 0)
    max_pos = np.take(input_data["transformer"]["tap_max"], 0)

    if min_pos > max_pos:
        min_pos, max_pos = max_pos, min_pos

    # initialize and create model
    model_tap = PGMcalculation()
    model_tap.create_pgm(input_network_data)

    transformer_id = model_tap.input_data["transformer"][0]["id"]

    # create power profile batch update data
    model_tap.create_batch_update_data(path_active_power_profile, path_reactive_power_profile)

    for tap_pos in range(min_pos, max_pos + 1):
        # create model update data:
        update_tap_pos = initialize_array("update", "transformer", 1)
        update_tap_pos["id"] = transformer_id
        update_tap_pos["tap_pos"] = [tap_pos]
        update_tap_data = {"transformer": update_tap_pos}

        model_tap.update_model(update_tap_data)
        model_tap.run_power_flow_calculation()

        if mode == 0:
            voltage_df = model_tap.aggregate_voltages()
        else:
            loading_df = model_tap.aggregate_line_loading()

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
            if tap_pos == min_pos:
                total_losses_min = total_losses_tap
                optimal_tap_pos_value = tap_pos
            elif total_losses_tap < total_losses_min:
                total_losses_min = total_losses_tap
                optimal_tap_pos_value = tap_pos

    return optimal_tap_pos_value
