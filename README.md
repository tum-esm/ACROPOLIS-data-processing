# ACROPOLIS Visualisation & Processing

The ACROPOLIS (**A**utonomous and **C**alibrated **R**oof-top **O**bservatory for Metro**POLI**ton **S**ensing) network spans 20 roof-top CO2 sensor systems located in and around Munich and is part of the EU Horizon Project [ICOS Cities](https://www.icos-cp.eu/projects/icos-cities). The network is run by the open source software [ACROPOLIS-edge](https://github.com/tum-esm/ACROPOLIS-edge).

This repository contains a Python based processing pipeline and a collection of Jupyter notebooks to visualise ACROPOLIS network data.

The repository is work in progress and can experience breaking changes.

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

## Initial Setup

- Navigate to `pipeline/config/`
- Copy `config.template.json` → Rename it to `config.json`
- Fill `config.json` with relevant info

## Running the scripts

(1) Process downloaded ThingsBoard data

```bash
python pipeline/acropolis_postprocessing.py
```

(2) Despike output from `acropolis_postprocessing.py`

```bash
python pipeline/timeseries_despiking.py
```

(3) Concat output from `timeseries_despiking.py` to site specific time series and export to csv format + HEADER for ICOS Cities portal

```bash
python pipeline/L1_icos_cities_portal.py
```

---

## Running Type Checks (MyPy)

To ensure type safety and catch potential errors, run:

```bash
bash scripts/run_mypy.sh
```
