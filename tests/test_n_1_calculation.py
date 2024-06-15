from power_system_simulation.n_1_calculation import (n_1_calc,InvalidLineIDError,LineAlreadyDisconnected,LineNotConnectedError)
import pandas as pd
import pytest

PTH_INPUT_NETWORK_DATA = "tests/data/small_network/input/input_network_data.json"
PTH_ACTIVE_PROFILE = "tests/data/small_network/input/active_power_profile.parquet"
PTH_REACTIVE_PROFILE = "tests/data/small_network/input/reactive_power_profile.parquet"

def test_Invalid_line_error():
    model = n_1_calc()
    model.setup_model(PTH_INPUT_NETWORK_DATA,PTH_ACTIVE_PROFILE,PTH_REACTIVE_PROFILE)
    with pytest.raises(InvalidLineIDError):
        model.n_1_calculation(-1)

def test_already_disconnected():
    model = n_1_calc()
    model.setup_model(PTH_INPUT_NETWORK_DATA,PTH_ACTIVE_PROFILE,PTH_REACTIVE_PROFILE)
    with pytest.raises(LineAlreadyDisconnected):
        model.n_1_calculation(24)

def test_n_1_calculation():
    model = n_1_calc()
    model.setup_model(PTH_INPUT_NETWORK_DATA,PTH_ACTIVE_PROFILE,PTH_REACTIVE_PROFILE)
    result = model.n_1_calculation(18, True) 
    assert isinstance(result, pd.DataFrame)
    assert "Max_Loading" in result.columns
    assert not result.empty

def test_line_line_not_connected():
    model = n_1_calc()
    model.setup_model(PTH_INPUT_NETWORK_DATA,PTH_ACTIVE_PROFILE,PTH_REACTIVE_PROFILE)
    with pytest.raises(LineNotConnectedError):
        model.n_1_calculation(200) # need to find a non connected line