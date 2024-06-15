"""
This module provides the PGMfunctions class which includes methods for creating a PGM model, 
validating input data, creating batch update data, calculating EV penetration levels, performing 
N-1 calculations, and finding optimal tap positions in a power grid model.

It also defines custom exceptions for handling missing profile data.
"""

from power_grid_model.utils import json_deserialize

from power_system_simulation.ev_penetration_level import ev_penetration_calculation
from power_system_simulation.graph_processing import GraphProcessor
from power_system_simulation.input_data_validity_check import reformat_pgm_to_array, validate_input_data
from power_system_simulation.n_1_calculation import n_1_calculation_module
from power_system_simulation.optimal_tap_position import optimal_tap_pos
from power_system_simulation.pgm_calculation_module import PGMcalculation


class NoActivePowerProfileProvided(Exception):
    """Raised when no active power profile is provided when it is needed"""


class NoReactivePowerProfileProvided(Exception):
    """Raised when no reactive power profile is provided when it is needed"""


class NoEVPowerProfileProvided(Exception):
    """Raised when no EV power profile is provided when it is needed"""


class NoMetaDataProvided(Exception):
    """Raised when no meta data is provided when it is needed"""


class PGMfunctions:
    """
    A class to handle various functions related to the power grid model (PGM).

    Methods
    -------
    __init__(self, path_input_network_data, path_active_power_profile=0,
    path_reactive_power_profile=0, path_ev_active_power_profile=0, path_meta_data=0)
        Initializes the PGMfunctions class with paths to data.

    create_pgm_model(self)
        Creates the PGM model instance and initializes the network array model.

    input_data_validity_check(self, test_case=0)
        Validates the input data paths and raises appropriate exceptions if any path is missing.

    create_batch_update_data(self, path_active_power_profile=0, path_reactive_power_profile=0)
        Creates batch update data using the provided active and reactive power profiles.

    ev_penetration_level(self, penetration_level_percentage, path_ev_power_profile=0,
    path_meta_data=0, assert_valid_pwr_profile=False)
        Calculates the EV penetration level based on the provided data.

    n_1_calculation(self, line_id_disconnect, reset_model_once_done=False)
        Performs N-1 calculation for a given line disconnection.

    find_optimal_tap_position(self, optimization_mode=0, path_active_power_profile=0,
    path_reactive_power_profile=0, threads=1)
        Finds the optimal tap position based on the provided data and optimization mode.
    """

    def __init__(
        self,
        path_input_network_data: str,
        path_active_power_profile=0,
        path_reactive_power_profile=0,
        path_ev_active_power_profile=0,
        path_meta_data=0,
    ):
        """
        Initializes the PGMfunctions class with paths to data.

        Parameters
        ----------
        path_input_network_data : str
            Path to the input network data file.
        path_active_power_profile : str, optional
            Path to the active power profile file (default is 0).
        path_reactive_power_profile : str, optional
            Path to the reactive power profile file (default is 0).
        path_ev_active_power_profile : str, optional
            Path to the EV active power profile file (default is 0).
        path_meta_data : str, optional
            Path to the meta data file (default is 0).
        """
        self.path_input_network_data = path_input_network_data
        self.path_active_power_profile = path_active_power_profile
        self.path_reactive_power_profile = path_reactive_power_profile
        self.path_ev_active_power_profile = path_ev_active_power_profile
        self.path_meta_data = path_meta_data

    def create_pgm_model(self):
        """
        Creates the PGM model instance and initializes the network array model.

        This method initializes the PGMcalculation model and processes the input network data
        to create an array model using GraphProcessor.
        """
        # Create PGM model instance
        self.model = PGMcalculation()
        self.model.create_pgm(self.path_input_network_data)

        # Create array model of network
        with open(self.path_input_network_data, encoding="utf-8") as ind:
            input_network = json_deserialize(ind.read())
        vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id = reformat_pgm_to_array(
            input_network
        )
        self.input_network_array_model = GraphProcessor(
            vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id
        )

    def run_single_powerflow_calculation(self):
        self.model.run_power_flow_calculation()
        voltages = self.model.aggregate_voltages()
        loadings = self.model.aggregate_line_loading()
        return voltages, loadings

    def input_data_validity_check(self, test_case=0):
        """
        Validates the input data paths and raises appropriate exceptions if any path is missing.

        Parameters
        ----------
        test_case : int, optional
            The test case number for validation purposes (default is 0).

        Raises
        ------
        NoActivePowerProfileProvided
            If the path to the active power profile is not provided.
        NoReactivePowerProfileProvided
            If the path to the reactive power profile is not provided.
        NoEVPowerProfileProvided
            If the path to the EV power profile is not provided.
        NoMetaDataProvided
            If the path to the meta data is not provided.
        """
        if 0 not in (
            self.path_active_power_profile,
            self.path_reactive_power_profile,
            self.path_ev_active_power_profile,
            self.path_meta_data,
        ):
            validate_input_data(
                self.path_input_network_data,
                self.path_active_power_profile,
                self.path_reactive_power_profile,
                self.path_ev_active_power_profile,
                self.path_meta_data,
                test_case=test_case,
            )
        else:
            if self.path_active_power_profile == 0:
                raise NoActivePowerProfileProvided("No path to active power profile was provided")
            if self.path_reactive_power_profile == 0:
                raise NoReactivePowerProfileProvided("No path to reactive power profile was provided")
            if self.path_ev_active_power_profile == 0:
                raise NoEVPowerProfileProvided("No path to EV power profile was provided")
            if self.path_meta_data == 0:
                raise NoMetaDataProvided("No path to meta data was provided")

    def create_batch_update_data(self, path_active_power_profile=0, path_reactive_power_profile=0) -> None:
        """
        Creates batch update data using the provided active and reactive power profiles.

        Parameters
        ----------
        path_active_power_profile : str, optional
            Path to the active power profile file (default is 0).
        path_reactive_power_profile : str, optional
            Path to the reactive power profile file (default is 0).

        Raises
        ------
        NoActivePowerProfileProvided
            If the path to the active power profile is not provided.
        NoReactivePowerProfileProvided
            If the path to the reactive power profile is not provided.
        """
        if path_active_power_profile != 0:
            pth_active = path_active_power_profile
        elif self.path_active_power_profile != 0:
            pth_active = self.path_active_power_profile
        else:
            raise NoActivePowerProfileProvided("No path to active power profile was provided")

        if path_reactive_power_profile != 0:
            pth_reactive = path_reactive_power_profile
        elif self.path_reactive_power_profile != 0:
            pth_reactive = self.path_reactive_power_profile
        else:
            raise NoReactivePowerProfileProvided("No path to reactive power profile was provided")

        if 0 not in (pth_active, pth_reactive):
            self.model.create_batch_update_data(pth_active, pth_reactive)

    def ev_penetration_level(
        self,
        penetration_level_percentage: int,
        path_ev_power_profile=0,
        path_meta_data=0,
        assert_valid_pwr_profile=False,
    ):
        """
        Calculates the EV penetration level based on the provided data.

        Parameters
        ----------
        penetration_level_percentage : int
            The penetration level percentage of EVs.
        path_ev_power_profile : str, optional
            Path to the EV power profile file (default is 0).
        path_meta_data : str, optional
            Path to the meta data file (default is 0).
        assert_valid_pwr_profile : bool, optional
            Flag to assert valid power profile (default is False).

        Returns
        -------
        tuple
            DataFrames containing voltages and line loading.

        Raises
        ------
        NoEVPowerProfileProvided
            If the path to the EV power profile is not provided.
        NoReactivePowerProfileProvided
            If the path to the meta data is not provided.
        """
        if path_ev_power_profile != 0:
            pth_ev = path_ev_power_profile
        elif self.path_ev_active_power_profile != 0:
            pth_ev = self.path_ev_active_power_profile
        else:
            raise NoEVPowerProfileProvided("No path to EV power profile was provided")

        if path_meta_data != 0:
            pth_meta = path_meta_data
        elif self.path_meta_data != 0:
            pth_meta = self.path_meta_data
        else:
            raise NoReactivePowerProfileProvided("No path to meta data was provided")

        if 0 not in (pth_meta, pth_ev):
            df_voltages, df_line_loading = ev_penetration_calculation(
                self.model,
                self.input_network_array_model,
                self.path_input_network_data,
                pth_ev,
                pth_meta,
                penetration_level_percentage,
                assert_valid_pwr_profile=assert_valid_pwr_profile,
            )
            return df_voltages, df_line_loading

    def n_1_calculation(self, line_id_disconnect: int, reset_model_once_done=False):
        """
        Performs N-1 calculation for a given line disconnection.

        Parameters
        ----------
        line_id_disconnect : int
            The ID of the line to be disconnected.
        reset_model_once_done : bool, optional
            Flag to reset the model once the calculation is done (default is False).

        Returns
        -------
        table
            Results of the N-1 calculation.
        """
        return n_1_calculation_module(
            self.model, self.input_network_array_model, line_id_disconnect, reset_model_once_done=reset_model_once_done
        )

    def find_optimal_tap_position(
        self, optimization_mode=0, path_active_power_profile=0, path_reactive_power_profile=0, threads=1
    ):
        """
        Finds the optimal tap position based on the provided data and optimization mode.

        Parameters
        ----------
        optimization_mode : int, optional
            The optimization mode (default is 0), 0 for minimum voltage deviation, 1 for minimum line loading.
        path_active_power_profile : str, optional
            Path to the active power profile file (default is 0).
        path_reactive_power_profile : str, optional
            Path to the reactive power profile file (default is 0).
        threads : int, optional
            Number of threads to use for the calculation (default is 1).

        Returns
        -------
        int
            Optimal tap positions.

        Raises
        ------
        NoActivePowerProfileProvided
            If the path to the active power profile is not provided.
        NoReactivePowerProfileProvided
            If the path to the reactive power profile is not provided.
        """
        if path_active_power_profile != 0:
            pth_active = path_active_power_profile
        elif self.path_active_power_profile != 0:
            pth_active = self.path_active_power_profile
        else:
            raise NoActivePowerProfileProvided("No path to active power profile was provided")

        if path_reactive_power_profile != 0:
            pth_reactive = path_reactive_power_profile
        elif self.path_reactive_power_profile != 0:
            pth_reactive = self.path_reactive_power_profile
        else:
            raise NoReactivePowerProfileProvided("No path to reactive power profile was provided")

        if 0 in (path_active_power_profile, path_reactive_power_profile):
            self.create_batch_update_data(pth_active, pth_reactive)

        if 0 not in (pth_active, pth_reactive):
            return optimal_tap_pos(
                self.model, self.path_input_network_data, mode=optimization_mode, number_threads=threads
            )
