from power_system_simulation.ev_penetration_level import *

pth_input_network_data = "tests/data/small_network/input/input_network_data.json"
pth_ev_power = "tests/data/small_network/input/ev_active_power_profile.parquet"
pth_meta_data = "tests/data/small_network/input/meta_data.json"


def test_ev_penetration():
    voltages, loading = ev_penetration_calculation(pth_input_network_data, pth_ev_power, pth_meta_data, 100)
    assert voltages.shape == (960, 4)
    assert loading.shape == (9, 5)
