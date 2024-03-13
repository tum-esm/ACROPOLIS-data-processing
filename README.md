# ACROPOLIS Visualisation & Processing

The ACROPOLIS (**A**utonomous and **C**alibrated **R**oof-top **O**bservatory for Metro**POLI**ton **S**ensing) network spans 20 roof-top CO2 sensor systems located in and around Munich and is part of the EU Horizon Project [ICOS Cities](https://www.icos-cp.eu/projects/icos-cities). The network is run by the open source software [Hermes](https://github.com/tum-esm/hermes). 

This repository is a collection of Jupyter notebooks to pull, process and visualise ACROPOLIS network data.
The repository is work in progress and can experience breaking changes.

## Installation

```bash
python3.11 -m venv .venv
source .venv/bin/activate
poetry install
```

## Notebooks


```bash
📁 notebooks
    📄 processing_pipeline.ipynb
```
- Download a local copy from hermes db
- Process data from the PICARRO reference instrument
- Perform a Wet -> Dry conversion 
- Correct dry measurements with daily calibration
- Aggregate to 10m and 1h products

```bash
📁 notebooks
    📄 plot_raw_data.ipynb 
```

- Visualise all available sensor data

```bash
📁 notebooks
    📁 side-by-side_analysis
        📄 sbs_performance.ipynb
        📄 performance_scatter_plot.ipynb
```

- Calculate the MAE, RMSE during the side-by-side campaign
- Analyze dependencies on environmental parameters
