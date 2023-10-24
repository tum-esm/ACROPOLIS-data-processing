import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.express as px
import warnings
from typing import Optional

warnings.simplefilter("ignore", category=FutureWarning)


def plot_sensor_measurement(df, col_name: str, filter: Optional[str] = None):
    if filter != None:
        df = df.groupby_dynamic("creation_timestamp", every=filter).agg(
            [
                pl.all().exclude(["creation_timestamp"]).mean(),
            ]
        )

    fig = px.line(
        df,
        x="creation_timestamp",
        y=col_name,
        markers=True,
        title=col_name,
        color="system_name",
    )
    fig.show()
