'''provides an alternative grid topology if a line is disabled'''
import numpy as np
import pandas as pd
from power_grid_model import initialize_array
from power_grid_model.utils import json_deserialize

from power_system_simulation.graph_processing import GraphProcessor
from power_system_simulation.input_data_validity_check import reformat_pgm_to_array
from power_system_simulation.pgm_calculation_module import PGMcalculation


class InvalidLineIDError(Exception):
    """Error raised when the given line ID to be disconnected is not valid"""

class LineAlreadyDisconnected(Exception):
    """Error raised when the given line ID is already disconnected"""

class N1Calc:
    '''provides alternative grid topology if an line is disabled. seperated into 3 different functions for performance.
    set up model provides a pgm
    n_1_calculation provides an alternative topology
    create_line_update_data updates the data.
    '''
    def __init__(self):
        """Initializes class"""

    def setup_model(
        self, path_input_network_data: str, path_active_power_profile: str, path_reactive_power_profile: str
    ) -> None:
        '''takes 3 string with directions to network data, active and reactive power profile.
        returns a pgm'''
        # Create PGM model instance
        self.model_n1 = PGMcalculation()
        self.model_n1.create_pgm(path_input_network_data)

        # Create array model of network
        with open(path_input_network_data, encoding="utf-8") as ind:
            input_network = json_deserialize(ind.read())
        vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id = reformat_pgm_to_array(
            input_network
        )
        self.input_network_array_model = GraphProcessor(
            vertex_ids, edge_ids, edge_vertex_id_pairs, edge_enabled, source_vertex_id
        )

        self.model_n1.create_batch_update_data(path_active_power_profile, path_reactive_power_profile)

    def n_1_calculation(self, line_id_disconnect, reset_model_once_done=False):
        '''provides a alternative grid topology when an line is disconnected
        takes and line id '''
        # first error handling:
        line_id_index = np.where(self.input_network_array_model.edge_ids == line_id_disconnect)[0]
        if line_id_index.size > 0:
            line_id_index = line_id_index[0]
        else:
            raise InvalidLineIDError("Line ID to disconnect is not a valid line ID!")

        if self.input_network_array_model.edge_enabled[line_id_index] is False:
            raise LineAlreadyDisconnected("Line to be disconnected is already disconnected!")

        # TO ADD LineNotConnected ERROR

        # update model
        update_line_data = self.create_line_update_data(line_id_disconnect, 0)
        self.model_n1.update_model(update_line_data)

        # find alternative edges:
        alternative_lines = self.input_network_array_model.find_alternative_edges(line_id_disconnect)
        print(alternative_lines)

        # run time-series power flow for each alternative edge:
        result_table_list = []
        previous_line_id = None
        for alt_edge_id in alternative_lines:
            # change model with new lines:
            if previous_line_id is not None:
                update_line_data = self.create_line_update_data(previous_line_id, 0)
                self.model_n1.update_model(update_line_data)
            update_line_data = self.create_line_update_data(alt_edge_id, 1)
            self.model_n1.update_model(update_line_data)
            previous_line_id = alt_edge_id

            # run calculation
            self.model_n1.run_power_flow_calculation()
            max_line_loading = self.model_n1.aggregate_line_loading()

            # find maximum and create table row
            max_index = max_line_loading["Max_Loading"].idxmax()
            max_loading_timestamp = max_line_loading.at[max_index, "Max_Loading_Timestamp"]
            max_loading = max_line_loading.at[max_index, "Max_Loading"]
            result_table_list.append([alt_edge_id, max_loading, max_index, max_loading_timestamp])

        # reset last tested alternative edge:
        update_line_data = self.create_line_update_data(previous_line_id, 0)
        self.model_n1.update_model(update_line_data)

        max_min_line_loading_df_columns = [
            "Alternative_Line_ID",
            "Max_Loading",
            "Max_Loading_Line_ID",
            "Max_Loading_Timestamp",
        ]
        result_table = pd.DataFrame(result_table_list, columns=max_min_line_loading_df_columns)
        result_table.set_index("Alternative_Line_ID", inplace=True)

        # reset disabled edge to enabled again
        if reset_model_once_done:
            update_line_data = self.create_line_update_data(line_id_disconnect, 0)
            self.model_n1.update_model(update_line_data)

        return result_table

    def create_line_update_data(self, line_id_dis, to_status_line):
        '''Updates line date, takes and id and status and retuns and updated 
        '''
        update_line_dt = initialize_array("update", "line", 1)
        update_line_dt["id"] = [line_id_dis]  # change line ID 3
        update_line_dt["from_status"] = [to_status_line]
        update_line_dt["to_status"] = [to_status_line]

        update_line_data = {"line": update_line_dt}

        return update_line_data


PTH_INPUT_NETWORK_DATA = "tests/data/small_network/input/input_network_data.json"
PTH_ACTIVE_PROFILE = "tests/data/small_network/input/active_power_profile.parquet"
PTH_REACTIVE_PROFILE = "tests/data/small_network/input/reactive_power_profile.parquet"


N_1_MODEL = N1Calc()
N_1_MODEL.setup_model(PTH_INPUT_NETWORK_DATA, PTH_ACTIVE_PROFILE, PTH_REACTIVE_PROFILE)
print(
    N_1_MODEL.n_1_calculation(18, True)
) #True => resets model to initial state (disabled line is not actually disabled)
