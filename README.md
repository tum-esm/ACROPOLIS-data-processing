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
    📁 processing_pipeline.ipynb
```

- Process your local db copy from tum-esm/hermes
- Dynamically calculate slope and intrcept from daily calibrations
- Read and import Picarro measurement files

```bash
📁 notebooks
    📁 plot_all_data.ipynb
```

- Visualise all available sensor data
- Üerform calibration correction on sensor data
- Compare site-by-site data to Picarro reference measurement

```bash 
📁 notebooks
    📁 icos_calibration.ipynb
```

- Analyze the lab calibration