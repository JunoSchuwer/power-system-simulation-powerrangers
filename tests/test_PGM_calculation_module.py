import pandas as pd
import pytest
from power_grid_model import CalculationMethod, CalculationType, PowerGridModel, initialize_array, validation
from power_grid_model.utils import json_deserialize, json_serialize_to_file
from power_grid_model.validation import ValidationException, assert_valid_batch_data, assert_valid_input_data

from power_system_simulation.PGM_calculation_module import PGM_calculation, ProfileNotMatchingError


#test data
input_network_data = "src/data/input/input_network_data.json"
path_active_profile = "src/data/input/active_power_profile.parquet"
path_reactive_profile = "src/data/input/reactive_power_profile.parquet"

path_output_table_row_per_line="src/data/expected_output/output_table_row_per_line.parquet"
path_output_table_row_per_timestamp="src/data/expected_output/output_table_row_per_timestamp.parquet"

output_table_row_per_line=pd.read_parquet(path_output_table_row_per_line)
output_table_row_per_timestamp=pd.read_parquet(path_output_table_row_per_timestamp)



def test_PGM_calculation():
    max_min_voltages, max_min_line_loading=PGM_calculation(input_network_data, path_active_profile, path_reactive_profile)
    assert max_min_voltages.equals(output_table_row_per_timestamp)
    assert max_min_line_loading.equals(output_table_row_per_line)




max_min_voltages, max_min_line_loading=PGM_calculation(input_network_data, path_active_profile, path_reactive_profile)
#why
print(max_min_line_loading.compare(output_table_row_per_line, result_names=("our function", "correct")))

#lol i guess this is not the correct answer then
print(max_min_line_loading.loc[7, 'Total_Loss'])
print(output_table_row_per_line.loc[7, 'Total_Loss'])

test_PGM_calculation()