import os
import glob
from datetime import datetime
from datetime import timezone
import polars as pl
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.express as px

quickflow_directory = (
    "/Users/patrickaigner/Documents/PROJECTS/ACROPOLIS/Software/quickflow/acropolis/"
)
local_path = "/Users/patrickaigner/Documents/PROJECTS/ACROPOLIS/Software/ACROPOLIS-visualisation/data/"
picarro_path = "/Users/patrickaigner/Documents/PROJECTS/ACROPOLIS/Database/PICARRO"

df_parq = (
    pl.scan_parquet(os.path.join(quickflow_directory, "measurements.parquet"))
    .collect()
    .pivot(
        values="value",
        index=[
            "system_name",
            "revision",
            "creation_timestamp",
            "receipt_timestamp",
        ],
        columns="attribute",
        aggregate_function="first",
    )
)

df_parq.write_parquet(
    os.path.join(local_path, "pivot_measurements.parquet"),
    statistics=True,
)

df_parq = None
