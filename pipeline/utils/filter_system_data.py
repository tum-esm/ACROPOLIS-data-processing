import polars as pl
from .paths import AVERAGED_GASES

# load calibration bottle info (preprocessed)
df_gas = pl.read_csv(AVERAGED_GASES)


def extract_wind_data(df_raw: pl.LazyFrame) -> pl.DataFrame:
    #extract wind data from df_raw
    try:
        return df_raw.select(pl.col("datetime", "system_id", "^(wxt532_.*)$")) \
            .collect() \
            .filter(pl.col('wxt532_direction_avg') > 0) \
            .sort("datetime")
    except Exception:
        return pl.DataFrame()


def extraxt_auxilliary_data(df_raw: pl.LazyFrame) -> pl.DataFrame:
    #extract auxilliary data from df_raw
    try:
        return df_raw.select(pl.col("datetime", "system_id", "^(enclosure_.*)$", "^(raspi_.*)$", "^ups_.*$")) \
        .collect() \
        .filter(pl.col('enclosure_bme280_temperature') > 0) \
        .sort("datetime")
    except Exception:
        return pl.DataFrame()


def extract_edge_calibration_data(df_raw: pl.DataFrame) -> pl.DataFrame:
    #extract edge calibration data from df_raw
    try:
        return df_raw.select(pl.col("datetime", "system_id", "cal_gmp343_slope", "cal_gmp343_intercept", "cal_sht_45_offset")) \
        .collect() \
        .filter(pl.col('cal_gmp343_slope') > 0) \
        .sort("datetime")

    except Exception:
        return pl.DataFrame()


def extract_measurement_data(df_raw: pl.LazyFrame) -> pl.LazyFrame:
    #extract measurement data from df_raw
    return df_raw.sort("datetime") \
    .select(pl.all().exclude('^wxt532_.*$', '^cal_.*$', '^enclosure_.*$', '^raspi_.*$', '^ups_.*$')) \
    .filter(pl.col('gmp343_filtered') > 0.0) \
    .filter(pl.col('gmp343_temperature') > 0.0) \
    .filter(pl.col('sht45_humidity') > 0.0) \
    .filter(pl.col('bme280_pressure') > 0.0)


def extract_calibration_data(df_raw: pl.LazyFrame) -> pl.DataFrame:
    #extract calibration data from df_raw
    return df_raw.select("datetime","system_id", '^cal_.*$') \
    .collect() \
    .filter(pl.col("cal_bottle_id") > 0.0) \
    .filter(pl.col("cal_bottle_id") < float(df_gas["cal_bottle_id"].max())) \
    .filter(pl.col("cal_gmp343_filtered") > 0.0) \
    .filter(pl.col("cal_gmp343_temperature") > 0.0) \
    .filter(pl.col("cal_sht45_humidity") >= 0.0) \
    .filter(pl.col("cal_bme280_pressure") > 0.0)


def extract_years(df: pl.LazyFrame) -> list[int]:
    #extract years from df_raw
    return df["datetime"].dt.year().unique().to_list()
