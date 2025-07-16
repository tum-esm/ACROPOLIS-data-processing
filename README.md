# ACROPOLIS Visualization & Processing

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![mypy](https://github.com/tum-esm/ACROPOLIS-data-processing/actions/workflows/ci.yml/badge.svg)](https://github.com/tum-esm/ACROPOLIS-visualisation/actions)

The ACROPOLIS (**A**utonomous and **C**alibrated **R**oof-top **O**bservatory for Metro**POLI**ton **S**ensing) network spans 20 roof-top CO2 sensor systems located in and around Munich and is part of the EU Horizon Project [ICOS Cities](https://www.icos-cp.eu/projects/icos-cities). The network is run by the open source software [ACROPOLIS-edge](https://github.com/tum-esm/ACROPOLIS-edge).

Input data for the processing pipeline is downloaded from the [ThingsBoard](https://thingsboard.io) Open-Source IoT platform using the [ThingsBoard-Downloader](https://github.com/tum-esm/ThingsBoard-Downloader).

This repository contains a Python based processing pipeline realized with performant [Polars Dataframes](https://pola.rs) and a collection of Jupyter notebooks to visualise the ACROPOLIS network data. Python scripts are statically typed and continuously checked using [mypy](https://github.com/python/mypy) via GitHub Actions. 

#### Key Pipeline Features

- ✅ Customizable data processing via JSON configuration
- ✅ Efficient storage using Parquet files for fast querying
- ✅ Performant processing using Polars DataFrames
- ✅ Dilution and calibration correction of $CO_2$ data
- ✅ Time series outlier detection via Hampel filter
- ✅ Easy CSV reformatting and header generation to ICOS format
- ✅ Automated ICOS Cities data portal upload

<br/>

## Structure

- `data/`: Contains input and output data directories.
- `pipeline/`: Contains the processing pipeline scripts and configuration files.
- `notebooks/`: Contains Jupyter notebooks for data visualization.
- `scripts/`: Contains utility scripts for running type checks and other tasks.


<br/>

## Installation

### **Prerequisites**

- Python **3.12 or later**
- Poetry installed (`pip install poetry`)
- Local data from ThingsBoard-Downloader: [Repository](https://github.com/tum-esm/ThingsBoard-Downloader)


### **Set up the virtual environment and install dependencies**

```bash
python3 -m venv .venv  # Create virtual environment
source .venv/bin/activate  # Activate it
poetry install --with dev  # Install dependencies
```

## Run the pipeline

Instructions on how to set up the pipeline can be found in the pipeline [README](pipeline/README.md).

```bash
python pipeline/01_acropolis_postprocessing.py
python pipeline/02_timeseries_despiking.py
python pipeline/03_L1_write_csv_icos_cp.py
python pipeline/04_L1_upload_csv_icos_cp.py
```


## Run Type Checks (MyPy)

To ensure type safety and catch potential errors, run:

```bash
bash scripts/run_mypy.sh
```


## Related Work

Aigner et. al.: Advancing Urban Greenhouse Gas Monitoring: Development and Evaluation of a High-Density CO2 Sensor Network in Munich. ICOS Science Conference 2024, Versailles, France, 10.-12. Sept, [Link](https://www.icos-cp.eu/news-and-events/science-conference/icos2024sc/all-abstracts)

