# acropolis-visualisation & processing pipeline

A collection of Jupyter notebooks to pull, process and visualise ACROPOLIS network data.
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
- Read and correct (calibration) PICARRO data
- Dynamically calculate slope and intercept from daily calibrations
- Perform a Wet -> Dry conversion 
- Aggregate to 10m and 1h products

```bash
📁 notebooks
    📄 plot_raw_data.ipynb
```

- Visualise all available sensor data
