import os
import glob
import polars as pl
import time
import logging
from datetime import datetime

from utils.config_files import load_json_config
from utils.icos_cp_http_upload import upload_file_to_icos_cp

from utils.paths import CONFIG_DIRECTORY, LOG_DIRECTORY, ICOS_CITIES_LEVEL_1

assert (os.path.exists(ICOS_CITIES_LEVEL_1))

# load files
config = load_json_config("config.json")

path = os.path.join(CONFIG_DIRECTORY, "sites.csv")
sites_meta = pl.read_csv(path, separator=";").with_columns(
    pl.exclude(pl.Utf8).cast(str))

# load csv files
L1_filenames = glob.glob(os.path.join(ICOS_CITIES_LEVEL_1, '*.csv'))

# Create a log file with the current date (YYYY-MM-DD)
log_filename = os.path.join(LOG_DIRECTORY,
                            f"{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format=
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(
        )  # This allows logs to be printed to the console as well
    ])

logging.info("=========================================")
logging.info("Starting upload of *.csv files to ICOS Cities Portal")

# Record start time
start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logging.info(f"Script started at: {start_datetime}")

for path in L1_filenames:  
    root_directory = os.path.dirname(path)
    fname = os.path.basename(path)
    site_id = os.path.basename(path)[:4]
    
    logging.info(f"Processing site: {site_id}")

    if site_id == 'BLUT':
        sampling_height = float(os.path.basename(path)[5:7] + '.0')
    else:
        sampling_height = float(sites_meta.filter(
            pl.col("site") == site_id).select("height_of_building").item())

    logging.info(f"Created {fname}, {site_id}, {sampling_height}")
    
    logging.info(f"Uploading {fname} to ICOS Cities Portal")
    
    upload_file_to_icos_cp(config,
                        root_directory,
                        fname,
                        site_id,
                        sampling_height,
                        data_level=1)

# Record end time
end_time = time.time()
duration = end_time - start_time
end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logging.info(f"Script ended at: {end_datetime}")
logging.info(f"Total duration: {duration:.2f} seconds")