# Pipeline


- `01_acropolis_postprocessing.py`: Processes downloaded ThingsBoard data.
- `02_timeseries_despiking.py`: Despikes output from the postprocessing script.
- `03_L1_write_csv_icos_cp.py`: Concatenates system specific output from the despiking script to site-specific time series and exports to CSV format with a header for the ICOS Cities portal.
- `04_L1_upload_csv_icos_cp.py`: Uploads the CSV files to the ICOS Cities portal.

## Initial Setup

- Navigate to `pipeline/config/`
- Copy `config.template.json` → Rename it to `config.json`
- Fill `config.json` with relevant info

## Configuration

- `config.json` contains the configuration for the processing pipeline, including ICOS Cities data portal user credentials
- `sites_deployment_times.py` manages sensor deployment times
- `sites.csv` contains site metadata for the ICOS Cities portal 

