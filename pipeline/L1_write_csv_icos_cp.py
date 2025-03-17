import polars as pl
import os
import gc
import time
from datetime import datetime

from utils.config_files import load_json_config
from utils.import_data import import_acropolis_site_data
from utils.dataframe_operations import convert_to_1min_icos_cp_format
from utils.icos_cp_csv_conversion import df_to_L1_1min_icos_csv
from config.sites_deloyment_times import deployment_times

from utils.paths import DESPIKED_DATA_DIRECTORY, CONFIG_DIRECTORY, ICOS_CITIES_LEVEL_1

config = load_json_config("config.json")

path = os.path.join(CONFIG_DIRECTORY, "sites.csv")
sites_meta = pl.read_csv(path, separator=";").with_columns(
    pl.exclude(pl.Utf8).cast(str))

# Record start time
start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script started at: {start_datetime}")

for site in config["icos_cities_portal"]["site_names"]:
    
    df = import_acropolis_site_data(target_directory=DESPIKED_DATA_DIRECTORY,
                                    deployment_times=deployment_times,
                                    site_name=site)

    # Convert DF to ICOS CP format
    df = df.pipe(convert_to_1min_icos_cp_format)

    # Write to CSV with ICOS CP Header
    df_to_L1_1min_icos_csv(df=df, sites_meta=sites_meta, site=site)

    # Clear memory
    del df
    gc.collect()  # Explicitly run garbage collection

# Record end time
end_time = time.time()
duration = end_time - start_time
end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script ended at: {end_datetime}")
print(f"Total duration: {duration:.2f} seconds")
