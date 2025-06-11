import polars as pl
import gc
import os
import logging
import time
from hampel import hampel  # type: ignore
from datetime import datetime

from utils.config_files import load_json_config
from utils.import_data import import_acropolis_system_data
from utils.write_parquet import write_split_years
from utils.os_functions import ensure_data_dir

from utils.paths import DESPIKED_DATA_DIRECTORY, POSTPROCESSED_DATA_DIRECTORY, LOG_DIRECTORY

assert (os.path.exists(POSTPROCESSED_DATA_DIRECTORY))
ensure_data_dir(DESPIKED_DATA_DIRECTORY)
ensure_data_dir(LOG_DIRECTORY)

config = load_json_config("config.json")

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
logging.info("Starting despiking of ACROPOLIS postprocessed data")

# Record start time
start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logging.info(f"Script started at: {start_datetime}")

for id in config["despiking"]["system_ids"]:

    logging.info(f"Processing system with id: {id}")
    # Import system data
    df_raw = import_acropolis_system_data(
        years=config["despiking"]["input_years"],
        target_directory=os.path.join(POSTPROCESSED_DATA_DIRECTORY),
        id=id,
        prefix="1min")

    # Extract data
    selected_columns = [
        "datetime", "system_id", "system_name", "gmp343_corrected", "gmp343_edge_corrected",
        "gmp343_temperature", "sht45_humidity", "h2o_v%", "bme280_pressure",
        "enclosure_bme280_pressure", "wxt532_speed_avg", "wxt532_direction_avg", "gmp343_dry", "slope", "intercept", 
        "gmp343_edge_dry", "cal_gmp343_slope", "cal_gmp343_intercept"
    ]

    # Select columns of interest from postprocessed df
    df = df_raw.select(selected_columns) \
        .filter(pl.col("gmp343_corrected") > 0) \
        .collect()

    # Convert CO2 column to numpy series
    data = df.get_column("gmp343_corrected").to_numpy()

    # Apply the Hampel filter
    result = hampel(data,
                    window_size=config["despiking"]["window_size"],
                    n_sigma=config["despiking"]["n_sigma"])

    # Print share of detected spikes
    logging.info(
        f"System ID: {id}, Detected spikes: {(len(result.outlier_indices) / len(data)):.4f}"
    )

    # Create column "Flag" = 'H' indicating local contamination
    df = df.with_columns(pl.Series("co2_hampel_filtered", result.filtered_data)) \
        .cast({"co2_hampel_filtered": pl.Float64}) \
        .with_columns(
        [
            pl.col("gmp343_corrected").round(2),
            pl.col("co2_hampel_filtered").round(2),
        ]
        ) \
        .with_columns(pl.when(pl.col("gmp343_corrected").ne(pl.col("co2_hampel_filtered"))).then(pl.lit('H')).otherwise(pl.lit('U')).alias("Flag")) \
        .drop("co2_hampel_filtered") 

    # Save data
    logging.info(f"Writing 1min despiked data to parquet. Length: {len(df)}")
    write_split_years(df=df,
                      id=id,
                      target_directory=DESPIKED_DATA_DIRECTORY,
                      prefix="flagged_L1_1_min")

    # Clear memory
    del df, data, result
    gc.collect()  # Explicitly run garbage collection

# Record end time
end_time = time.time()
duration = end_time - start_time
end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logging.info(f"Script ended at: {end_datetime}")
logging.info(f"Total duration: {duration:.2f} seconds")