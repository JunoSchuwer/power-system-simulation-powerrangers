import json
import math
import random

import pandas as pd
import power_grid_model as pgm #de vazut daca trebe

from power_grid_model import CalculationType, initialize_array
from power_grid_model.utils import json_deserialize
from power_grid_model.validation import assert_valid_batch_data

from power_system_simulation.graph_processing import GraphProcessor
from power_system_simulation.input_data_validity_check import reformat_pgm_to_array
from power_system_simulation.pgm_calculation_module import PGMcalculation

class LineIDValidation(Exception):
    """Error raised when the given line ID to be disconnected is not valid"""

class LineConnectionBothSides(Exception):
    """Error raised when the given line ID is not connected initially at both sides"""

def n_1_disconnect(self, lineID: int) -> None:

    
    return lineID #placeholder so I don't get identation error
