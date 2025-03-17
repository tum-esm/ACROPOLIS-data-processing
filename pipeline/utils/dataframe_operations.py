import polars as pl
from datetime import timedelta
from .datetime_conversions import calculate_decimal_year
from typing import Literal


def join_slice(df: pl.DataFrame, df_slice: pl.DataFrame,
               tolerance: str) -> pl.DataFrame:
    if len(df_slice) > 0:
        return df.join_asof(df_slice,
                            on="datetime",
                            strategy="nearest",
                            tolerance=tolerance)
    else:
        return df


def concat_dataframe(
    df1: pl.DataFrame, df2: pl.DataFrame,
    how: Literal['vertical', 'vertical_relaxed', 'diagonal',
                 'diagonal_relaxed', 'horizontal', 'align', 'align_full',
                 'align_inner', 'align_left', 'align_right']
) -> pl.DataFrame:
    return pl.concat([df1, df2], how=how)

def convert_to_1min_icos_cp_format(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({"gmp343_corrected": "co2", "h2o_v%": "h2o", "enclosure_bme280_pressure":"pressure", "gmp343_temperature":"sensor_temperature", "wxt532_speed_avg": "ws", "wxt532_direction_avg":"wd"}) \
            .with_columns((pl.col("datetime") + timedelta(seconds=30))) \
            .with_columns(
                pl.col("co2").round(2),
                pl.col("h2o").round(2),
                pl.col("pressure").round(2),
                pl.col("sensor_temperature").round(2),
                pl.col("ws").round(2),
                pl.col("wd").round(2)) \
            .with_columns(
                (pl.col("datetime").dt.year()).alias("Year"),
                (pl.col("datetime").dt.month()).alias("Month"),
                (pl.col("datetime").dt.day()).alias("Day"),
                (pl.col("datetime").dt.hour()).alias("Hour"),
                (pl.col("datetime").dt.minute()).alias("Minute"),
                (pl.col("datetime").dt.second()).alias("Second"),
                (pl.col('datetime').dt.to_string("%Y-%m-%d %H:%M:%S")).alias("#Datetime")) \
            .with_columns(pl.struct(['datetime']) \
            .map_elements(lambda x: calculate_decimal_year(x['datetime']), return_dtype=pl.Float64) \
            .alias("DecimalDate")) \
            .select(["#Datetime", "Year", "Month", "Day", "Hour", "Minute", "Second", "DecimalDate", "co2", "h2o", "pressure", "sensor_temperature", "ws", "wd", "Flag"]) \
            .with_columns(pl.exclude(pl.Utf8).cast(str)) \
            .fill_null('')

def convert_to_1h_icos_cp_format(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({"gmp343_corrected": "co2", "h2o_v%": "h2o", "enclosure_bme280_pressure":"pressure", "gmp343_temperature":"sensor_temperature", "wxt532_speed_avg": "ws", "wxt532_direction_avg":"wd"}) \
        .with_columns((pl.col("datetime") + timedelta(minutes=30))) \
        .with_columns(
            pl.col("co2").round(2),
            pl.col("h2o").round(2),
            pl.col("pressure").round(2),
            pl.col("sensor_temperature").round(2),
            pl.col("ws").round(2),
            pl.col("wd").round(2),
            pl.col("Stdev").round(2)) \
        .with_columns(
            (pl.col("creation_timestamp").dt.year()).alias("Year"),
            (pl.col("creation_timestamp").dt.month()).alias("Month"),
            (pl.col("creation_timestamp").dt.day()).alias("Day"),
            (pl.col("creation_timestamp").dt.hour()).alias("Hour"),
            (pl.col("creation_timestamp").dt.minute()).alias("Minute"),
            (pl.col("creation_timestamp").dt.second()).alias("Second"),
            (pl.col('creation_timestamp').dt.to_string("%Y-%m-%d %H:%M:%S")).alias("#Datetime")) \
        .with_columns(pl.struct(['creation_timestamp']) \
        .map_elements(lambda x: calculate_decimal_year(x['creation_timestamp']), return_dtype=pl.Float64) \
        .alias("DecimalDate")) \
        .select(["#Datetime", "Year", "Month", "Day", "Hour", "Minute", "Second", "DecimalDate", "co2", "h2o", "pressure", "sensor_temperature", "ws", "wd", "NbPoints", "Stdev", "Flag"]) \
        .with_columns(pl.exclude(pl.Utf8).cast(str)) \
        .fill_null('')