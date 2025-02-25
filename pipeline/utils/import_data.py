import polars as pl
import glob
import os
from datetime import datetime

from .config_files import load_json_config

config = load_json_config("config.json")


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
        path = os.path.join(target_directory, str(year), file_name)
        if os.path.exists(path):
            paths += sorted(glob.glob(path), key=os.path.getmtime)

    for path in paths:
        if os.path.isfile(path):
            df_years.append(pl.scan_parquet(path))

    return pl.concat(df_years,
                     how="diagonal").with_columns(system_id=pl.lit(id))


def import_acropolis_site_data(target_directory: str, deployment_times: dict,
                               site_name: str):
    extracted_dates = []

    for sensor in deployment_times[site_name]["sensors"]:

        id = sensor["id"]

        start_time = datetime.strptime(sensor["start_time"],
                                       "%Y-%m-%dT%H:%M:%S%z")
        end_time = datetime.strptime(sensor["end_time"], "%Y-%m-%dT%H:%M:%S%z")

        df_temp = import_acropolis_system_data(
            years=config["icos_cities_portal"]["input_years"],
            target_directory=os.path.join(target_directory),
            id=id,
            prefix="flagged_L1_1_min") \
            .with_columns(pl.col("datetime").cast(pl.Datetime("us")).dt.replace_time_zone("UTC")) \
            .filter(pl.col("datetime") \
                .is_between(start_time, end_time)) \
                .collect()

        extracted_dates.append(df_temp)

    return pl.concat(extracted_dates, how="diagonal")
