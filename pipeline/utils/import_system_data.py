import polars as pl
import glob
import os


# Import system specific processed data
def import_acropolis_system_data(years: list[int],
                                 target_directory: str,
                                 id: int,
                                 prefix: str = None) -> pl.LazyFrame:
    paths = []
    df_years = []

    if prefix:
        file_name = f"{prefix}_acropolis-{id}.parquet"
    else:
        file_name = f"acropolis-{id}.parquet"

    for year in years:
        paths += sorted(glob.glob(
            os.path.join(target_directory, str(year), file_name)),
                        key=os.path.getmtime)

    for path in paths:
        if os.path.isfile(path):
            df_years.append(pl.scan_parquet(path))

    return pl.concat(df_years,
                     how="diagonal").with_columns(system_id=pl.lit(id))
