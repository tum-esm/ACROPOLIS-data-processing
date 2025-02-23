import polars as pl
import glob
import os

from utils.paths import THINGSBOARD_DATA_DIRECTORY


# Import system specific raw data
def import_acropolis_system_data(years: list[int], id: int) -> pl.LazyFrame:
    paths = []
    df_years = []

    for year in years:
        paths += sorted(glob.glob(
            os.path.join(THINGSBOARD_DATA_DIRECTORY, str(year),
                         f"acropolis-{id}.parquet")),
                        key=os.path.getmtime)

    for path in paths:
        df_years.append(pl.scan_parquet(path))

    return pl.concat(df_years,
                     how="diagonal").with_columns(system_id=pl.lit(id))
