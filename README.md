# acropolis-visualisation

A collection of Jupyter notebooks to pull and visualise ACROPOLIS network data.

## Installation

```bash
python3.11 -m venv .venv
source .venv/bin/activate
poetry install
```

## Notebooks

```bash
📁 notebooks
    📁 notebooks
        📄 download_from_hermes.ipynb
```

- Download latest data from hermes db and perform pivot
- Add new data to local database

```bash
📁 notebooks
    📄 processing_pipeline.ipynb
```

- Download a local copy from hermes db
- Dynamically calculate slope and intrcept from daily calibrations
- Read and calibration correct Picarro measurement files

```bash
📁 notebooks
    📄 plot_all_data.ipynb
```

- Visualise all available sensor data
- Perform calibration correction on sensor data
- Compare site-by-site data to Picarro reference measurement
