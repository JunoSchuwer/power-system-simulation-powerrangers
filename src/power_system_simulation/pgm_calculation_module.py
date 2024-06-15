"""
power-grid-model calculation module

Raises:
    ProfilesNotMatchingError:  if the two profiles does not have matching timestamps and/or load ids
    ValidationException: if input data is invalid or if batch dataset is invalid

Returns:
    pd.dataFrame -> max/min line loading over entire time span
    pd.dataFrame -> max/min node voltage for each moment in time
"""

import numpy as np
import pandas as pd
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import assert_valid_batch_data, assert_valid_input_data


class ProfileTimestampsNotMatchingError(Exception):
    """Error raised when active and reactive load profiles do not have matching timestamps"""


class ProfileLoadIDsNotMatchingError(Exception):
    """Error raised when active and reactive load profiles do not have matching Load IDs"""


class PGMcalculation:
    """
    A class to perform power grid model calculations including creating models, updating data,
    running power flow calculations, and aggregating results.

    Methods
    -------
    __init__()
        Initializes the pgm_calculation class.
    create_pgm(input_network_data: str)
        Creates a power grid model from the input network data.
    create_batch_update_data(path_active_power_profile: str, path_reactive_power_profile: str)
        Creates batch update data for the model using the provided power profiles.
    update_model(model_update_data)
        Updates the model with new data.
    run_power_flow_calculation(update_data_calc=0)
        Runs the power flow calculation using the model.
    aggregate_voltages()
        Aggregates the voltage data from the power flow calculation.
    aggregate_line_loading()
        Aggregates the line loading data from the power flow calculation.
    """

    def __init__(self):
        """Initializes the pgm_calculation class with no parameters."""

    def create_pgm(self, input_network_data: str):
        """
        Creates a power grid model from the input network data.

        Parameters
        ----------
        input_network_data : str
            Path to the JSON file containing the input network data.
        """
        # Deserialize JSON file of network
        with open(input_network_data, encoding="utf-8") as ind:
            self.input_data = json_deserialize(ind.read())

        # Validate input data, raises `ValidationException`
        assert_valid_input_data(input_data=self.input_data, calculation_type=CalculationType.power_flow)

        # Construct model
        self.model = PowerGridModel(input_data=self.input_data)

    def create_batch_update_data(self, path_active_power_profile: str, path_reactive_power_profile: str):
        """
        Creates batch update data for the model using the provided power profiles.

        Parameters
        ----------
        path_active_power_profile : str
            Path to the parquet file containing the active power profile.
        path_reactive_power_profile : str
            Path to the parquet file containing the reactive power profile.

        Raises
        ------
        ProfileLoadIDsNotMatchingError
            If the load IDs of active and reactive power profiles do not match.
        ProfileTimestampsNotMatchingError
            If the timestamps of active and reactive power profiles do not match.
        """
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

        assert_valid_batch_data(
            input_data=self.input_data, update_data=self.update_data, calculation_type=CalculationType.power_flow
        )

    def update_model(self, model_update_data):
        """
        Updates the model with new data.

        Parameters
        ----------
        model_update_data : dict
            The update data to be applied to the model.
        """
        assert_valid_batch_data(
            input_data=self.input_data, update_data=model_update_data, calculation_type=CalculationType.power_flow
        )
        self.model.update(update_data=model_update_data)

    def run_power_flow_calculation(self, update_data_calc=0, timestamps_given=0, threads=1):
        """
        Runs the power flow calculation using the model.

        Parameters
        ----------
        update_data_calc : dict, optional
            The update data to be used for the calculation. If not provided, it defaults to self.update_data.
        """
        if update_data_calc == 0:
            update_data_calc = self.update_data
        if not isinstance(timestamps_given, int):
            self.timestamps = timestamps_given
        self.output_data = self.model.calculate_power_flow(
            update_data=update_data_calc, calculation_method=CalculationMethod.newton_raphson, threading=threads
        )

    def aggregate_voltages(self):
        """
        Aggregates the voltage data from the power flow calculation.

        Returns
        -------
        pd.DataFrame
            DataFrame containing timestamps, maximum voltages,
            and minimum voltages along with their respective node IDs.
        """
        u_pu, node_id = (
            self.output_data["node"]["u_pu"],
            self.output_data["node"]["id"],
        )

        # Find max and min voltages along with their node indices
        u_pu_max = u_pu.max(axis=1)
        u_pu_max_indices = np.argmax(u_pu, axis=1)
        u_pu_min = u_pu.min(axis=1)
        u_pu_min_indices = np.argmin(u_pu, axis=1)

        # Use the indices to get the corresponding node IDs
        u_pu_max_node_ids = np.take_along_axis(node_id, u_pu_max_indices[:, None], axis=1).flatten()
        u_pu_min_node_ids = np.take_along_axis(node_id, u_pu_min_indices[:, None], axis=1).flatten()

        # Construct the DataFrame
        max_min_voltage_df = pd.DataFrame(
            {
                "Timestamp": self.timestamps,
                "Max_Voltage": u_pu_max,
                "Max_Voltage_Node": u_pu_max_node_ids,
                "Min_Voltage": u_pu_min,
                "Min_Voltage_Node": u_pu_min_node_ids,
            }
        )

        max_min_voltage_df.set_index("Timestamp", inplace=True)

        return max_min_voltage_df

    def aggregate_line_loading(self):
        """
        Aggregates the line loading data from the power flow calculation.

        Returns
        -------
        pd.DataFrame
            DataFrame containing line IDs, total losses, maximum loading,
            and minimum loading along with their respective timestamps.
        """
        line_loading = self.output_data["line"]["loading"]
        line_id = self.output_data["line"]["id"]
        p_from = self.output_data["line"]["p_from"]
        p_to = self.output_data["line"]["p_to"]
        timestamps = self.timestamps

        unique_line_ids = np.unique(line_id)

        # Calculate losses for each line over time
        e_losses = np.abs(p_from + p_to)  # p_from and p_to are in Watts
        e_losses_kwh = np.trapz(e_losses, axis=0) / 1000  # integrate over time and convert to kWh

        # Initialize arrays to store results
        max_loadings = np.zeros_like(unique_line_ids, dtype=float)
        max_loading_timestamps = np.zeros_like(unique_line_ids, dtype="datetime64[ns]")
        min_loadings = np.zeros_like(unique_line_ids, dtype=float)
        min_loading_timestamps = np.zeros_like(unique_line_ids, dtype="datetime64[ns]")

        # Iterate over unique line IDs
        for idx, line_id_i in enumerate(unique_line_ids):
            line_mask = line_id == line_id_i
            loads = line_loading[line_mask]

            # Calculate max and min loadings and their timestamps
            max_loadings[idx] = loads.max()
            max_loading_timestamps[idx] = timestamps[np.argmax(loads)]
            min_loadings[idx] = loads.min()
            min_loading_timestamps[idx] = timestamps[np.argmin(loads)]

        # Construct the DataFrame
        max_min_line_loading_df = pd.DataFrame(
            {
                "Line_ID": unique_line_ids,
                "Total_Loss": e_losses_kwh,
                "Max_Loading": max_loadings,
                "Max_Loading_Timestamp": max_loading_timestamps,
                "Min_Loading": min_loadings,
                "Min_Loading_Timestamp": min_loading_timestamps,
            }
        )

        max_min_line_loading_df.set_index("Line_ID", inplace=True)

        return max_min_line_loading_df
