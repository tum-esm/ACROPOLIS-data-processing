import polars as pl
import polars.selectors as cs
import gc
import time
import os
from datetime import datetime

from utils.config_files import load_json_config
from utils.import_data import import_acropolis_system_data
from utils.filter_system_data import extract_wind_data, extraxt_auxilliary_data, extract_edge_calibration_data, extract_measurement_data, extract_calibration_data
from utils.dilution_correction import wet_to_dry_mole_fraction
from utils.calibration_processing import calculate_slope_intercept, apply_slope_intercept
from utils.write_parquet import write_split_years

from utils.paths import POSTPROCESSED_DATA_DIRECTORY, THINGSBOARD_DATA_DIRECTORY

config = load_json_config("config.json")

# Record start time
start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script started at: {start_datetime}")

for id in config["postprocessing"]["system_ids"]:
    print("Processing system with id:", id)
    # Import system data
    df_raw = import_acropolis_system_data(
        years=config["postprocessing"]["input_years"],
        target_directory=THINGSBOARD_DATA_DIRECTORY,
        id=id,
    )

    # Extract data
    df = extract_measurement_data(df_raw)
    df_wind = extract_wind_data(df_raw)
    df_aux = extraxt_auxilliary_data(df_raw)
    df_edge_cal = extract_edge_calibration_data(df_raw)
    df_calibration = extract_calibration_data(df_raw)

    # Calculate slope and intercept
    df_slope_intercept = calculate_slope_intercept(df_calibration)

    del df_calibration
    gc.collect()  # Explicitly run garbage collection

    # Aggregate to 1 minute intervals
    df = df.group_by_dynamic("datetime", every='1m', group_by=["system_id", "system_name"]) \
            .agg(cs.numeric().mean())

    # Process measurement data
    df = df.pipe(wet_to_dry_mole_fraction) \
        .pipe(apply_slope_intercept, df_slope_intercept) \
        .join_asof(df_wind, on="datetime", strategy="nearest", tolerance="2m") \
        .join_asof(df_aux, on="datetime", strategy="nearest", tolerance="2m") \
        .join_asof(df_edge_cal, on="datetime", strategy="nearest", tolerance="1d") \
        .drop("^.*_right$") \
        .collect()

    # Save data
    print("Writing 1m data to parquet. Length:", len(df))
    write_split_years(df=df,
                      id=id,
                      target_directory=POSTPROCESSED_DATA_DIRECTORY,
                      prefix="1min")

    # Aggregate to 1 hour intervals
    df_1h = df.sort("datetime") \
            .group_by_dynamic("datetime", every='1h', group_by=["system_id", "system_name"]) \
            .agg([
                  cs.numeric().mean(),
                  pl.col("gmp343_corrected").std().alias("gmp343_corrected_std"),
                  pl.col("gmp343_corrected").var().alias("gmp343_corrected_var")
              ])

    # Save data
    print("Writing 1h data to parquet. Length:", len(df_1h))
    write_split_years(df=df_1h,
                      id=id,
                      target_directory=POSTPROCESSED_DATA_DIRECTORY,
                      prefix="1h")

    # Clear memory
    del df, df_1h
    gc.collect()  # Explicitly run garbage collection

# Record end time
end_time = time.time()
duration = end_time - start_time
end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script ended at: {end_datetime}")
print(f"Total duration: {duration:.2f} seconds")

# import plotly.express as px
# fig = px.line(
#     df_1h,
#     x="datetime",
#     y=["cal_gmp343_slope", "wxt532_direction_avg", "wxt532_speed_avg"])
# fig.show()
