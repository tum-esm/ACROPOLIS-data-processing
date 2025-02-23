import warnings
import polars as pl
import numpy

from .paths import AVERAGED_GASES

warnings.simplefilter("ignore", category=FutureWarning)

# load calibration bottle concentrations (preprocessed)
df_gas = pl.read_csv(AVERAGED_GASES)


# define functions
def process_bottle(data: list, ignore_len: bool = False):
    if ignore_len:
        x = data[int(len(data) * 0.3):int(len(data) * 0.95)]
        return numpy.median(x)
    # 2nd bottle
    if 50 < len(data) < 70:
        x = data[int(len(data) * 0.3):int(len(data) * 0.95)]
        return numpy.median(x)
    # 1st bottle
    elif 70 < len(data) < 130:
        x = data[int(len(data) * 0.5):int(len(data) * 0.95)]
        return numpy.median(x)
    else:
        return 0.0


def two_point_calibration(measured_values: list, true_values: list):
    # Check if input lists have length 2
    if len(measured_values) != 2 or len(true_values) != 2:
        return 0, 0

    # Calculate calibration parameters (slope and intercept)

    slope = (true_values[1] - true_values[0]) / (measured_values[1] -
                                                 measured_values[0])
    # y_true = m * y_meas + t
    intercept = true_values[0] - slope * measured_values[0]

    return {"slope": slope, "intercept": intercept}


def calculate_slope_intercept(df: pl.DataFrame) -> pl.DataFrame:
    return df.join(df_gas.cast({"cal_bottle_id": pl.Float64}), on=["cal_bottle_id"], how="left", coalesce=True) \
    .with_columns((pl.col("datetime").dt.date()).alias("date")) \
    .sort("date") \
    .group_by(["date", "system_id", "cal_bottle_id"]) \
    .agg([
        pl.col("cal_gmp343_filtered"),
        pl.col("cal_bottle_CO2").last(),
        pl.col("datetime").last(),
    ]) \
    .with_columns([
        pl.col("cal_gmp343_filtered").map_elements(lambda x: process_bottle(x), return_dtype=pl.Float64)
    ]) \
    .filter(pl.col("cal_gmp343_filtered") > 0) \
    .sort(pl.col("cal_gmp343_filtered")) \
    .group_by(["date", "system_id"]) \
    .agg([
        pl.col("cal_gmp343_filtered"),
        pl.col("cal_bottle_CO2"),
        pl.col("datetime").last()
    ]) \
    .filter(pl.col("cal_gmp343_filtered").list.len() == 2) \
    .with_columns(
    pl.struct(["cal_gmp343_filtered", "cal_bottle_CO2"])
    .map_elements(lambda x: two_point_calibration(x["cal_gmp343_filtered"], x["cal_bottle_CO2"]), return_dtype=pl.Struct)
    .alias("slope_intercept"))  \
    .with_columns([
        pl.col("slope_intercept").struct.field("slope").alias("slope"),
        pl.col("slope_intercept").struct.field("intercept").alias("intercept")
    ]) \
    .select("datetime", "system_id", "slope", "intercept") \
    .filter(pl.col("slope") > 0.9) \
    .filter(pl.col("slope") < 1.1) \
    .sort("datetime")


def apply_slope_intercept(df: pl.DataFrame,
                          df_slope_intercept: pl.DataFrame) -> pl.DataFrame:
    return df.sort("datetime") \
        .join_asof(df_slope_intercept, on="datetime", strategy="nearest", tolerance="10m") \
        .with_columns([
            pl.col("slope").interpolate().alias("slope_interpolated"),
            pl.col("intercept").interpolate().alias("intercept_interpolated")
            ]) \
        .with_columns([
            pl.col("slope_interpolated").forward_fill().backward_fill(),
            pl.col("intercept_interpolated").forward_fill().backward_fill(),
            ]) \
        .with_columns(((pl.col("gmp343_dry")) * pl.col("slope_interpolated") + pl.col("intercept_interpolated")).alias("gmp343_corrected"))
