# ACROPOLIS Visualisation & Processing

The ACROPOLIS (**A**utonomous and **C**alibrated **R**oof-top **O**bservatory for Metro**POLI**ton **S**ensing) network spans 20 roof-top CO2 sensor systems located in and around Munich and is part of the EU Horizon Project [ICOS Cities](https://www.icos-cp.eu/projects/icos-cities). The network is run by the open source software [Hermes](https://github.com/tum-esm/hermes). 

This repository is a collection of Jupyter notebooks to download, process and visualise ACROPOLIS network data.

The repository is work in progress and can experience breaking changes.

## Installation

```bash
python3.11 -m venv .venv
source .venv/bin/activate
poetry install
```

Generate and fill an *.env* file based on the *.env.example*
```bash
📁 root
    📄 .env.example
    📄 .env
```

