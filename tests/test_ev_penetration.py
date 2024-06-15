from power_system_simulation.ev_penetration_level import *

model_ev = 
model_ev_arrays = 
pth_input_network_data = "tests/data/small_network/input/input_network_data.json"
pth_ev_power = "tests/data/small_network/input/ev_active_power_profile.parquet"
pth_meta_data = "tests/data/small_network/input/meta_data.json"
penetration_level_percentage=100

def test_ev_penetration():
    voltages, loading = ev_penetration_calculation(model_ev,
                                                   model_ev_arrays,
                                                   pth_input_network_data, 
                                                   pth_ev_power, 
                                                   pth_meta_data, 
                                                   penetration_level_percentage)
    assert voltages.shape == (960, 4)
    assert loading.shape == (9, 5)
