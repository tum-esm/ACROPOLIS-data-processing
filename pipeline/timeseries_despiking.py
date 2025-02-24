import polars as pl
import gc
import os
import time
from hampel import hampel
from datetime import datetime

from utils.config_files import load_json_config
from utils.import_system_data import import_acropolis_system_data
from utils.write_parquet import write_split_years

from utils.paths import DESPIKED_DATA_DIRECTORY, POSTPROCESSED_DATA_DIRECTORY

config = load_json_config("config.json")

# Record start time
start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script started at: {start_datetime}")

for id in config["postprocessing"]["system_ids"]:

    print("Processing system with id:", id)
    # Import system data
    df_raw = import_acropolis_system_data(
        years=config["despiking"]["input_years"],
        target_directory=os.path.join(POSTPROCESSED_DATA_DIRECTORY),
        id=id,
        prefix="1min")

    # Extract data
    selected_columns = [
        "datetime", "system_id", "system_name", "gmp343_corrected",
        "gmp343_edge_corrected", "gmp343_temperature", "h2o_v%",
        "bme280_pressure", "enclosure_bme280_pressure", "wxt532_speed_avg",
        "wxt532_direction_avg"
    ]

    #CO2 column is cast to f32 to match the hampel filter output, else comparison fails
    df = df_raw.select(selected_columns) \
        .cast({"gmp343_corrected": pl.Float32}) \
        .filter(pl.col("gmp343_corrected") > 0) \
        .collect()

    # Convert CO2 column to pandas series
    data = df.get_column("gmp343_corrected").to_pandas()

    # Apply the Hampel filter
    result = hampel(data,
                    window_size=config["despiking"]["window_size"],
                    n_sigma=config["despiking"]["n_sigma"])

    # Print share of detected spikes
    print(
        f"System ID: {id}, Detected spikes: {(len(result.outlier_indices) / len(data)):.4f}"
    )

    # Create column "Flag" = 'H' indicating local contamination
    df = df.with_columns((pl.from_pandas(result.filtered_data)).alias("co2_hampel_filtered")) \
        .with_columns(pl.when(pl.col("gmp343_corrected").ne(pl.col("co2_hampel_filtered"))).then(pl.lit('H')).otherwise(pl.lit('U')).alias("Flag")) \
        .drop("co2_hampel_filtered") \
        .cast({"gmp343_corrected": pl.Float64})

    # Save data
    print("Writing 1min despiked data to parquet. Length:", len(df))
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

print(f"Script ended at: {end_datetime}")
print(f"Total duration: {duration:.2f} seconds")
