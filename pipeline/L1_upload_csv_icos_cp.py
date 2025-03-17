import os
import glob
import polars as pl
import time
from datetime import datetime

from utils.config_files import load_json_config
from utils.icos_cp_http_upload import upload_file_to_icos_cp

from utils.paths import CONFIG_DIRECTORY, ICOS_CITIES_LEVEL_1

config = load_json_config("config.json")

# Load cites metadata
path = os.path.join(CONFIG_DIRECTORY, "sites.csv")
sites_meta = pl.read_csv(path, separator=";").with_columns(
    pl.exclude(pl.Utf8).cast(str))

# load csv files
L1_filenames = glob.glob(os.path.join(ICOS_CITIES_LEVEL_1, '*.csv'))

# Record start time
start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script started at: {start_datetime}")

for path in L1_filenames:  
    root_directory = os.path.dirname(path)
    fname = os.path.basename(path)
    site_id = os.path.basename(path)[:4]

    if site_id == 'BLUT':
        sampling_height = float(os.path.basename(path)[5:7] + '.0')
    else:
        sampling_height = float(sites_meta.filter(
            pl.col("site") == site_id).select("height_of_building").item())

    print(root_directory, fname, site_id, sampling_height)
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

print(f"Script ended at: {end_datetime}")
print(f"Total duration: {duration:.2f} seconds")