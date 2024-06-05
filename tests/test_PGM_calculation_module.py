import pandas as pd
import pytest
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array, validation
from power_grid_model.utils import json_deserialize, json_serialize_to_file
from power_grid_model.validation import ValidationException, assert_valid_batch_data, assert_valid_input_data

from power_system_simulation.PGM_calculation_module import PGM_calculation, ProfileNotMatchingError


#test data
input_network_data = "tests/data/input/input_network_data.json"
path_active_profile = "tests/data/input/active_power_profile.parquet"
path_reactive_profile = "tests/data/input/reactive_power_profile.parquet"

path_output_table_row_per_line="tests/data/expected_output/output_table_row_per_line.parquet"
path_output_table_row_per_timestamp="tests/data/expected_output/output_table_row_per_timestamp.parquet"

output_table_row_per_line=pd.read_parquet(path_output_table_row_per_line)
output_table_row_per_timestamp=pd.read_parquet(path_output_table_row_per_timestamp)



def test_PGM_calculation():
    max_min_voltages, max_min_line_loading=PGM_calculation(input_network_data, path_active_profile, path_reactive_profile)
    assert max_min_voltages.equals(output_table_row_per_timestamp)
    assert (max_min_line_loading.round(13)).equals(output_table_row_per_line.round(13)) #round since 14th number after comma is different

def test_invalid_input_data():
    pass