"""
power-grid-model calculation module

Raises:
    ProfilesNotMatchingError:  if the two profiles does not have matching timestamps and/or load ids
    ValidationException: if input data is invalid or if batch dataset is invalid

Returns:
    pd.dataFrame -> max/min line loading over entire time span
    pd.dataFrame -> max/min node voltage for each moment in time
"""

from typing import Tuple

import numpy as np
import pandas as pd
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import assert_valid_batch_data, assert_valid_input_data


class ProfileTimestampsNotMatchingError(Exception):
    """Error raised when active and reactive load profiles do not have matching timestamps"""


class ProfileLoadIDsNotMatchingError(Exception):
    """Error raised when active and reactive load profiles do not have matching Load IDs"""


class pgm_calculation:
    def __init__(self):
        pass
        
    def create_pgm(self, input_network_data: str):
        # deserialize json file of network
        with open(input_network_data, encoding="utf-8") as ind:
            self.input_data = json_deserialize(ind.read())
        
        # validate input data, raises `ValidationException`
        assert_valid_input_data(input_data=self.input_data, calculation_type=CalculationType.power_flow)

        # construct model
        self.model = PowerGridModel(input_data=self.input_data)
    
    def create_batch_update_data(self, path_active_power_profile: str, path_reactive_power_profile: str):
        active_power_profile = pd.read_parquet(path_active_power_profile)
        reactive_power_profile = pd.read_parquet(path_reactive_power_profile)

        self.timestamps = active_power_profile.index

        if (active_power_profile.columns != reactive_power_profile.columns).any():
            raise ProfileLoadIDsNotMatchingError("Load IDs of active and reactive power do not match!")
        if not active_power_profile.index.equals(reactive_power_profile.index):
            raise ProfileTimestampsNotMatchingError("Timestamps of active and reactive power do not match!")
        
        batch_update_dataset = initialize_array("update", "sym_load", active_power_profile.shape)
        batch_update_dataset["id"] = active_power_profile.columns.to_numpy()
        batch_update_dataset["p_specified"] = active_power_profile.to_numpy()
        batch_update_dataset["q_specified"] = reactive_power_profile.to_numpy()
        self.update_data = {"sym_load": batch_update_dataset}

        assert_valid_batch_data(input_data=self.input_data, update_data=self.update_data, calculation_type=CalculationType.power_flow)
    
    def update_model(self, model_update_data):
        assert_valid_batch_data(input_data=self.input_data, update_data=model_update_data, calculation_type=CalculationType.power_flow)
        self.model.update(update_data=model_update_data)

    def run_power_flow_calculation(self, update_data_calc=0):
        if update_data_calc==0:
            update_data_calc=self.update_data
        self.output_data = self.model.calculate_power_flow(
            update_data=update_data_calc, calculation_method=CalculationMethod.newton_raphson
        )
    
    def aggregate_voltages(self):
        u_pu, node_id = (
            self.output_data["node"]["u_pu"],
            self.output_data["node"]["id"],
        )

        max_min_voltages = []
        for timestamp_idx, timestamp in enumerate(self.timestamps):
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
        max_min_voltage_df_columns = [
            "Timestamp",
            "Max_Voltage",
            "Max_Voltage_Node",
            "Min_Voltage",
            "Min_Voltage_Node",
        ]
        max_min_voltage_df = pd.DataFrame(max_min_voltages, columns=max_min_voltage_df_columns)
        max_min_voltage_df.set_index("Timestamp", inplace=True)

        return max_min_voltage_df
    
    def aggregate_line_loading(self):
        line_loading, line_id, p_from, p_to = (
            self.output_data["line"]["loading"],
            self.output_data["line"]["id"],
            self.output_data["line"]["p_from"],
            self.output_data["line"]["p_to"],
        )

        max_min_line_loading = []
        for id_idx, line_id_i in enumerate(np.unique(line_id)):
            loads = line_loading[line_id == line_id_i]
            load_max, time_load_max, load_min, time_load_min = (
                loads.max(),
                self.timestamps[loads.argmax()],
                loads.min(),
                self.timestamps[loads.argmin()],
            )
            e_losses = abs(p_from[:, id_idx] + p_to[:, id_idx])  # p_from and p_to are in Watts
            e_losses_kwh = np.trapz(e_losses) / 1000
            max_min_line_loading.append([line_id_i, e_losses_kwh, load_max, time_load_max, load_min, time_load_min])

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

        return max_min_line_loading_df
