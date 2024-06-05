import json
import os

import numpy as np
import pandas as pd
import pytest
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array, validation
from power_grid_model.utils import json_deserialize, json_serialize_to_file
from power_grid_model.validation import ValidationException, assert_valid_batch_data, assert_valid_input_data

from power_system_simulation.pgm_calculation_module import ProfilesNotMatchingError, pgm_calculation

# test data
input_network_data = "tests/data/input/input_network_data.json"
path_active_profile = "tests/data/input/active_power_profile.parquet"
path_reactive_profile = "tests/data/input/reactive_power_profile.parquet"

path_output_table_row_per_line = "tests/data/expected_output/output_table_row_per_line.parquet"
path_output_table_row_per_timestamp = "tests/data/expected_output/output_table_row_per_timestamp.parquet"

output_table_row_per_line = pd.read_parquet(path_output_table_row_per_line)
output_table_row_per_timestamp = pd.read_parquet(path_output_table_row_per_timestamp)

# make folder location for incorrect data:
incorrect_folder_path = "tests/data/incorrect_input_data"
if not os.path.exists(incorrect_folder_path):
    os.makedirs(incorrect_folder_path)


def test_pgm_calculation():
    max_min_voltages, max_min_line_loading = pgm_calculation(
        input_network_data, path_active_profile, path_reactive_profile
    )
    assert max_min_voltages.equals(output_table_row_per_timestamp)
    assert (max_min_line_loading.round(13)).equals(
        output_table_row_per_line.round(13)
    )  # round since 14th number after comma is different


def test_invalid_input_data():
    # create invalid input data and save as "incorrect_input_network_data.json"
    with open(input_network_data) as ind:
        input_data_new = json.load(ind)
    input_data_new["data"]["node"][0]["id"] = 2  # node id=1 changed to node id=2
    path_incorrect_input_network_data = "tests/data/incorrect_input_data/incorrect_input_network_data.json"
    with open(path_incorrect_input_network_data, "w") as json_file:
        json.dump(input_data_new, json_file, indent=4)

    # test the incorrect input data
    with pytest.raises(ValidationException):
        max_min_voltages, max_min_line_loading = pgm_calculation(
            path_incorrect_input_network_data, path_active_profile, path_reactive_profile
        )


def test_invalid_batch_dataset():
    # create invalid batch dataset
    incorrect_active_power_profile = pd.read_parquet(path_active_profile)
    incorrect_reactive_power_profile = pd.read_parquet(path_reactive_profile)
    incorrect_active_power_profile = incorrect_active_power_profile.rename(columns={8: 222})
    incorrect_reactive_power_profile = incorrect_active_power_profile.rename(columns={8: 222})

    path_incorrect_active_profile = (
        "tests/data/incorrect_input_data/incorrect_batch_dataset_active_power_profile.parquet"
    )
    path_incorrect_reactive_profile = (
        "tests/data/incorrect_input_data/incorrect_batch_dataset_reactive_power_profile.parquet"
    )
    incorrect_active_power_profile.to_parquet(path_incorrect_active_profile, engine="pyarrow", compression="snappy")
    incorrect_reactive_power_profile.to_parquet(path_incorrect_reactive_profile, engine="pyarrow", compression="snappy")

    # test wrong batch update (batch update sym_load id not in input network)
    with pytest.raises(ValidationException):
        max_min_voltages, max_min_line_loading = pgm_calculation(
            input_network_data, path_incorrect_active_profile, path_incorrect_reactive_profile
        )


def test_load_ids_not_matching_error():
    # create incorrect load ID in active power profile and output new parquet file
    incorrect_active_power_profile = pd.read_parquet(path_active_profile)
    incorrect_active_power_profile = incorrect_active_power_profile.rename(columns={8: 222})
    path_incorrect_active_profile = "tests/data/incorrect_input_data/incorrect_loadIDs_active_power_profile.parquet"
    incorrect_active_power_profile.to_parquet(path_incorrect_active_profile, engine="pyarrow", compression="snappy")

    # test incorrect active profile
    with pytest.raises(ProfilesNotMatchingError):
        max_min_voltages, max_min_line_loading = pgm_calculation(
            input_network_data, path_incorrect_active_profile, path_reactive_profile
        )


def test_time_stamp_ids_not_matching_error():
    # create incorrect timestamp in active power profile and output new parquet file
    incorrect_active_power_profile = pd.read_parquet(path_active_profile)
    incorrect_active_power_profile.rename(
        index={pd.Timestamp("2024-01-01 00:00:00"): pd.Timestamp("2000-01-01 00:00:00")}, inplace=True
    )
    path_incorrect_active_profile = "tests/data/incorrect_input_data/incorrect_timestamps_active_power_profile.parquet"
    incorrect_active_power_profile.to_parquet(path_incorrect_active_profile, engine="pyarrow", compression="snappy")

    # test incorrect active profile
    with pytest.raises(ProfilesNotMatchingError):
        max_min_voltages, max_min_line_loading = pgm_calculation(
            input_network_data, path_incorrect_active_profile, path_reactive_profile
        )
