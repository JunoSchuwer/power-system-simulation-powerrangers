# Assignment 2: Power Grid Model
import numpy as np
import power_grid_model as pgm
import pandas as pd
from typing import Dict
from power_grid_model.utils import json_deserialize
from power_grid_model import (
    PowerGridModel,
    CalculationType,
    CalculationMethod,
    initialize_array
)
from power_grid_model.validation import (
    assert_valid_input_data,
    assert_valid_batch_data
)
'''
In Assignment 2 we are going to write a power grid calculation module
with [`power-grid-model`](https://power-grid-model.readthedocs.io/en/stable/) as the calculation core.
We describe here the input of your task, and the expected functionalities.

**You need to define the proper APIs including input data arguments for your package!**
'''
## Input data
class ValidationException(Exception):
  pass

'''
For this assignment, you need to handle the following input.

* A power grid in PGM input format
* A table containing active load profile of all the `sym_load` in the grid, with timestamps and load ids.
* A table containing reactive load profile of all the `sym_load` in the grid, with timestamps and load ids.
* The above two tables has the same number of rows and columns. The timestamps and load ids will be matching.
'''
def power_grid_calc(input_network_data: Dict)->Dict:

  with open(input_network_data) as ind:
    input_data=json_deserialize(ind.read())

  #validate input data
  assert_valid_input_data(input_data=input_data, calculation_type=CalculationType.power_flow)

  #construct model
  model = PowerGridModel(input_data=input_data)
  print(model.all_component_count)

  return

input_network_data = "src/data/input/input_network_data.json"
power_grid_calc(input_network_data)



## Functionalities
'''
We expect that you implement the following functionalities, including some error handling.
You need to test the error handling which is explicitely listed.

* Construct PGM using the input data.
  * Raise (passthrough the `ValidationException`) error if the input data is invalid.
* Create a PGM batch update dataset with the active and reactive load profiles.
  * Raise error if the two profiles does not have matching timestamps and/or load ids.
* Run time-series (batch) power flow calculation.
  * Raise (passthrough the `ValidationException`) error if the batch dataset is invalid.
* Aggregate the power flow results in the following two tables:
  * A table with each row representing a timestamp, with the following columns:
    * Timestamp (index column)
    * Maximum p.u. voltage of all the nodes for this timestamp
    * The node ID with the maximum p.u. voltage
    * Minimum p.u. voltage of all the nodes for this timestamp
    * The node ID with the minimum p.u. voltage
  * A table with each row representing a line, with the following columns:
    * Line ID (index column)
    * Energy loss of the line across the timeline in kWh (pay attention to unit conversions!)
      * You need to use the descrete numerical integral with [Trapezoidal rule](https://en.wikipedia.org/wiki/Trapezoidal_rule).
    * Maximum loading in p.u. of the line across the whole timeline
    * Timestamp of this maximum loading moment
    * Minimum loading in p.u. of the line across the whole timeline
    * Timestamp of this minimum loading moment

## Test datasets

We provide a test dataset with input and expected output in [SharePoint](https://tuenl.sharepoint.com/:f:/s/5XWG0-PowerSystemCalculationandSimualtion/EmbTZJRBIflBtkTm8txF-kcBgitgYLmJZz75Dwv9L5bZxw?e=2s8fyt).

To read the PGM JSON file into PGM input format in the memory, you need to use [PGM Serialization](https://power-grid-model.readthedocs.io/en/stable/examples/Serialization%20Example.html).

To read the load profiles into tables in the memory, you need to read from [`parquet`](https://parquet.apache.org/) files.
For example, you can use [`pandas.read_parquet`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_parquet.html)
'''