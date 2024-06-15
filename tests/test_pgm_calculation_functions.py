PTH_INPUT_NETWORK_DATA = "tests/data/small_network/input/input_network_data.json"
PTH_ACTIVE_PROFILE = "tests/data/small_network/input/active_power_profile.parquet"
PTH_REACTIVE_PROFILE = "tests/data/small_network/input/reactive_power_profile.parquet"
PTH_EV_ACTIVE_POWER_PROFILE = "tests/data/small_network/input/ev_active_power_profile.parquet"
PTH_META_DATA = "tests/data/small_network/input/meta_data.json"


PGM_MODEL=PGMfunctions(PTH_INPUT_NETWORK_DATA,PTH_ACTIVE_PROFILE,PTH_REACTIVE_PROFILE,PTH_EV_ACTIVE_POWER_PROFILE,PTH_META_DATA)
PGM_MODEL.input_data_validity_check()
PGM_MODEL.create_pgm_model()
PGM_MODEL.create_batch_update_data(PTH_ACTIVE_PROFILE, PTH_REACTIVE_PROFILE)
PGM_MODEL.ev_penetration_level(50, PTH_EV_ACTIVE_POWER_PROFILE, PTH_META_DATA, assert_valid_pwr_profile=True)
PGM_MODEL.n_1_calculation(18,True)
PGM_MODEL.find_optimal_tap_position()