import polars as pl

from utils.config_files import load_json_config
from utils.import_system_data import import_acropolis_system_data
from utils.filter_system_data import extract_wind_data, extraxt_auxilliary_data, extract_edge_calibration_data, extract_measurement_data, extract_calibration_data
from utils.dilution_correction import wet_to_dry_mole_fraction
from utils.calibration_processing import calculate_slope_intercept, apply_slope_intercept

from utils.paths import PIPELINE_OUTPUT_DIRECTORY

config = load_json_config("config.json")

id = 6
print_steps = True

# Import system data
df_raw = import_acropolis_system_data(years=config["input_years"], id=id)

# Extract data
df_wind = extract_wind_data(df_raw)
df_aux = extraxt_auxilliary_data(df_raw)
df_edge_cal = extract_edge_calibration_data(df_raw)
df_measurement = extract_measurement_data(df_raw)
df_calibration = extract_calibration_data(df_raw)

# Calculate slope and intercept
df_slope_intercept = calculate_slope_intercept(df_calibration)

# Aggregate to 1 minute intervals
df = df_measurement.group_by_dynamic("datetime", every='1m', group_by=["system_id", "system_name"]) \
        .agg(pl.all().exclude(["datetime","system_id", "system_name"]).mean())

# Process measurement data
df = df.pipe(wet_to_dry_mole_fraction) \
    .pipe(apply_slope_intercept, df_slope_intercept) \
    .join_asof(df_wind, on="datetime", strategy="nearest") \
    .join_asof(df_aux, on="datetime", strategy="nearest") \
    .join_asof(df_edge_cal, on="datetime", strategy="nearest") \
    .drop("^.*_right$")

# Aggregate to 10 minute intervals
df_10min = df.sort("datetime") \
        .group_by_dynamic("datetime", every='10m', group_by=["system_id", "system_name"]) \
        .agg(pl.all().exclude(["datetime","system_name"]).mean(),
                pl.col("gmp343_corrected").std().alias("std"),
                pl.col("gmp343_corrected").var().alias("var"))

# Aggregate to 1 hour intervals
df_1h = df.sort("datetime") \
        .group_by_dynamic("datetime", every='1h', group_by=["system_id", "system_name"]) \
        .agg(pl.all().exclude(["datetime","system_name"]).mean(),
                pl.col("gmp343_corrected").std().alias("std"),
                pl.col("gmp343_corrected").var().alias("var"))

# import plotly.express as px
# fig = px.line(
#     df_1h,
#     x="datetime",
#     y=["cal_gmp343_slope", "wxt532_direction_avg", "wxt532_speed_avg"])
# fig.show()
