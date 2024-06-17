# power-system-simulation
This is a student project for Power System Simulation.

## background
With the ongoing energy transition, the smart grid concept is crucial for medium voltage (MV) and low voltage (LV) distribution grids to handle the increasing integration of renewable energy sources, electric vehicles, and heat pumps. This project addresses these challenges by combining electrical power engineering and software engineering to develop tools for efficient power system analysis and grid optimization.
## Project Overview
The project module consists of three core components:
1.	Graph Processing
2.	Power Grid Calculation
3.	LV Grid Analytics
These components offer a complete toolkit for modern power system analysis, utilizing efficient algorithms and data-driven methods. This module ensures accurate power flow analysis and robust error handling. All function take .json and .parquet files as an input, specific limitation are handled by errors but general limitation are all ID’s need to be positive unique integers and list need to have matching timestamps and ID’s.
### Graph Processing
The Graph Processing module includes functionalities for creating and analysing undirected graphs:
•	Find Downstream Vertices: Identify all vertices downstream of a given vertex.
•	Find Alternative Edges: Find alternative paths or edges in the graph.
These functionalities are implemented with efficient graph algorithms, ensuring scalability for large datasets.
### Power Grid Calculation
The Power Grid Calculation module handles power flow analysis for given power grid data:
•	Input Data Handling: Process power grid data in PGM format along with active and reactive load profiles.
•	Functionalities:
o	Construct and validate the power grid model.
o	Create batch updates with load profiles.
o	Perform time-series power flow calculations.
o	Aggregate results into tables for voltage metrics and line metrics.
### LV Grid Analytics
The LV Grid Analytics module provides tools for analysing low voltage grids:
•	Input Data Handling: Process LV grid data, load profiles, and EV charging profiles.
•	Functionalities:
o	Validate input data.
o	Analyse EV penetration levels and run power flow calculations.
o	Optimize transformer tap positions.
o	Perform N-1 calculations to identify alternative grid topologies.
These tools enables optimization of LV grids, supporting the integration of renewable energy and electric vehicles.

## Installation
To get started clone the repository and install the necessary dependacies, usage of a vm is recommended.
```shell
pip install -e .[dev,example]
```

## Usage
Below a few examples are highlighted how the package is used. note that a wildcard import is not optimal, for performance reasons only import the necesarry functions.
```shell
from power_system_simulation.pgm_calculation_functions import *
PTH_INPUT_NETWORK_DATA = "tests/data/small_network/input/input_network_data.json"
PTH_ACTIVE_PROFILE = "tests/data/small_network/input/active_power_profile.parquet"
PTH_REACTIVE_PROFILE = "tests/data/small_network/input/reactive_power_profile.parquet"
PTH_EV_ACTIVE_POWER_PROFILE = "tests/data/small_network/input/ev_active_power_profile.parquet"
PTH_META_DATA = "tests/data/small_network/input/meta_data.json"
```
Where PTH_INPUT_NETWORK_DATA is the path to your input network data, PTH_ACTIVE_PROFILE is the path to your file containing active load profiles etc.
```shell
PGM_MODEL=PGMfunctions(PTH_INPUT_NETWORK_DATA,PTH_ACTIVE_PROFILE,PTH_REACTIVE_PROFILE,PTH_EV_ACTIVE_POWER_PROFILE,PTH_META_DATA)
```
The line above is used to intialize the functions then each PGM_MODEL.function runs one of the functions for example:
```shell
PGM_MODEL.input_data_validity_check()
```
checks input data validity.
```shell
PGM_MODEL.create_pgm_model()
```
creates a power grid model.
```shell
PGM_MODEL.create_batch_update_data()
```
Updates data
```shell
PGM_MODEL.run_single_powerflow_calculation()
```
runs a single powerflow calculation
```shell
PGM_MODEL.ev_penetration_level(50, assert_valid_pwr_profile=True)
```
runs an ev penetration level calculation, where 50 is the percentage of EV penetration, the second variable checks the input data validity.
```shell
PGM_MODEL.n_1_calculation(18,True)
``` 
Returns an alternative grid topology is line 18 is disconnected. Assingment 1 uses different data than assingment 2 and 3 so a function exist to convert between the two, if you just want to use the algoritmes developed for assingment 1 use the examples below
```shell
alternative_edges=PGM_MODEL.input_network_array_model.find_alternative_edges(18)
downstream_nodes= PGM_MODEL.input_network_array_model.find_downstream_vertices(16)
```
Other functions exist as introduced before in section project overview and have unique input variables but variable names are discriptive of the function.

## Contributing
We welcome contributions to improve this project module. Please get in contact with the course instructor, power-grid-model package creators or one of the students.

## License
This project is licensed under the BSD-3 License. See the LICENSE file for details.
Authors and Acknowledgments
Course Instructors: 
Yu (Tony) Xiang
Peter Salemink
students: 
Maarten de Rooij 
Juno Schuwer
Tudor Pioarǎ
Zoé Nézet 