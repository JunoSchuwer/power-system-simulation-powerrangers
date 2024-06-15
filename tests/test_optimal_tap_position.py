import pytest

from power_system_simulation.optimal_tap_position import *

pth_input_network_data = "tests/data/small_network/input/input_network_data.json"
pth_active_profile = "tests/data/small_network/input/active_power_profile.parquet"
pth_reactive_profile = "tests/data/small_network/input/reactive_power_profile.parquet"


def test_optimal_tap_position_mode_0():
    pos = optimal_tap_pos(pth_input_network_data, pth_active_profile, pth_reactive_profile, 0)
    assert pos == 1


def test_optimal_tap_position_mode_1():
    pos = optimal_tap_pos(pth_input_network_data, pth_active_profile, pth_reactive_profile, 1)
    assert pos == 5


def test_invalid_mode():
    with pytest.raises(InvalidMode):
        pos = optimal_tap_pos(pth_input_network_data, pth_active_profile, pth_reactive_profile, 2)
