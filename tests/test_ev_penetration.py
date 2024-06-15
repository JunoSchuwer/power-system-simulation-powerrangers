from power_system_simulation.ev_penetration_level import *

def test_ev_penetration():
    assert voltages.shape == (960, 4)
    assert loading.shape == (9, 5)
