"""provides an alternative grid topology if a line is disabled"""

import numpy as np
import pandas as pd
from power_grid_model import initialize_array


class InvalidLineIDError(Exception):
    """Error raised when the given line ID to be disconnected is not valid"""


def n_1_calculation_module(model_n1, input_network_array_model, line_id_disconnect, reset_model_once_done=False):
    """provides a alternative grid topology when an line is disconnected
    takes and line id"""
    # first error handling:
    line_id_index = np.where(input_network_array_model.edge_ids == line_id_disconnect)[0]
    if line_id_index.size > 0:
        line_id_index = line_id_index[0]
    else:
        raise InvalidLineIDError("Line ID to disconnect is not a valid line ID!")

    # update model
    update_line_data = create_line_update_data(line_id_disconnect, 0)
    model_n1.update_model(update_line_data)

    # find alternative edges:
    alternative_lines = input_network_array_model.find_alternative_edges(line_id_disconnect)

    # run time-series power flow for each alternative edge:
    result_table_list = []
    previous_line_id = None
    for alt_edge_id in alternative_lines:
        # change model with new lines:
        if previous_line_id is not None:
            update_line_data = create_line_update_data(previous_line_id, 0)
            model_n1.update_model(update_line_data)
        update_line_data = create_line_update_data(alt_edge_id, 1)
        model_n1.update_model(update_line_data)
        previous_line_id = alt_edge_id

        # run calculation
        model_n1.run_power_flow_calculation()
        max_line_loading = model_n1.aggregate_line_loading()

        # find maximum and create table row
        max_index = max_line_loading["Max_Loading"].idxmax()
        max_loading_timestamp = max_line_loading.at[max_index, "Max_Loading_Timestamp"]
        max_loading = max_line_loading.at[max_index, "Max_Loading"]
        result_table_list.append([alt_edge_id, max_loading, max_index, max_loading_timestamp])

    # reset last tested alternative edge:
    if previous_line_id is not None:
        update_line_data = create_line_update_data(previous_line_id, 0)
        model_n1.update_model(update_line_data)

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
        update_line_data = create_line_update_data(line_id_disconnect, 0)
        model_n1.update_model(update_line_data)

    return result_table


def create_line_update_data(line_id_dis, to_status_line):
    """Updates line date, takes and id and status and returns update data"""
    update_line_dt = initialize_array("update", "line", 1)
    update_line_dt["id"] = [line_id_dis]  # change line ID 3
    update_line_dt["from_status"] = [to_status_line]
    update_line_dt["to_status"] = [to_status_line]

    update_line_data = {"line": update_line_dt}

    return update_line_data
