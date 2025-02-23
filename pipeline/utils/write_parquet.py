import polars as pl
import os

from utils.filter_system_data import extract_years
from utils.os_functions import ensure_data_dir


def write_split_years(df: pl.DataFrame,
                      target_directory: str,
                      id: int,
                      prefix: str = None) -> None:
    for year in extract_years(df):
        data_path = os.path.join(target_directory, str(year))
        ensure_data_dir(data_path)

        if prefix is not None:
            df.filter(pl.col("datetime").dt.year() == year).write_parquet(
                os.path.join(data_path, f"{prefix}_acropolis-{id}.parquet"),
                row_group_size=100_000)
        else:
            df.filter(pl.col("datetime").dt.year() == year).write_parquet(
                os.path.join(data_path, f"acropolis-{id}.parquet"),
                row_group_size=100_000)
