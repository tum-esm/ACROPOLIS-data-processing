import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.express as px
import math


def plot_sensor_measurement(
    df,
    col_name: str,
    col_time: str = "creation_timestamp",
    sensor_id: list = [],
    filter=None,
):
    for id in sensor_id:
        df_f = df.clone()
        df_f = (
            df_f.filter(pl.col("sensor_name") == f"tum-esm-midcost-raspi-{id}")
            .sort(col_time)
            .filter(pl.col(col_name) > 0)
            .filter(pl.col(col_name) < 700)
            .select(pl.col(col_time, col_name))
        )

        if filter != None:
            df_f = df_f.groupby_dynamic(col_time, every=filter).agg(
                pl.all().exclude(col_time).mean()
            )

        sns.lineplot(data=df_f.collect(), x=col_time, y=col_name, label=id)

    # set axes labels
    plt.xlabel("Time")
    plt.ylabel(col_name)
    plt.xticks(rotation=45)
    plt.legend(title="Systems", bbox_to_anchor=(1, 1))
    plt.title(label=col_name)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))

    # show the plot
    plt.show()


def plot_sensor_calibration(
    df,
    col_name: str,
    col_time: str = "creation_timestamp",
    sensor_id: list = [],
    filter=None,
    cut_below: float | None = None,
    cut_above: float | None = None,
):
    for id in sensor_id:
        df_f = df.clone()
        # select sensor
        df_f = df_f.filter(pl.col("sensor_name") == f"tum-esm-midcost-raspi-{id}").sort(
            col_time
        )
        # filter 0 entries (== sensor fails)
        df_f = df_f.filter(pl.col(col_name) > 0)
        # additional filters < and >
        if cut_below != None:
            df_f = df_f.filter(pl.col(col_name) > cut_below)

        if cut_above != None:
            df_f = df_f.filter(pl.col(col_name) < cut_above)

        # reduce df to relevant columns
        df_f = df_f.select(pl.col(col_time, col_name))

        # apply filter if configured
        if filter != None:
            df_f = df_f.groupby_dynamic(col_name, every=filter).agg(
                pl.all().exclude(col_name).mean()
            )

        # plot
        sns.lineplot(data=df_f.collect(), x=col_time, y=col_name)

        # set axes labels
        plt.xlabel("Time")
        plt.ylabel(col_name)
        plt.xticks(rotation=45)
        plt.title(label=f"Calibration: tum-esm-midcost-raspi-{id}")
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M"))

        # show the plot
        plt.show()


def plot_wind_rose(df, id: int, location: str):
    df_w = df.clone()
    # filter for system
    df_w = df_w.filter(pl.col("sensor_name") == f"tum-esm-midcost-raspi-{id}").filter(
        pl.col("wxt532_direction_avg") > 0
    )
    # create bins for wind direction
    df_w = df_w.with_columns(
        pl.col("wxt532_direction_avg")
        .apply(find_closest_cardinal_direction, return_dtype=float)
        .alias("cardinal_direction")
    )
    # create bins for wind speed
    df_w = df_w.with_columns(
        pl.col("wxt532_speed_avg")
        .apply(lambda t: math.ceil(t * 2) / 2, return_dtype=float)
        .alias("strength")
    )
    # groupby relevant bins
    df_w = df_w.groupby(["cardinal_direction", "strength"]).count().sort("strength")

    fig = px.bar_polar(
        df_w.collect(),
        r="count",
        theta="cardinal_direction",
        color="strength",
        template="seaborn",
    )

    fig.add_annotation(text="Calm", x=0.5, y=0.5, showarrow=False, font=dict(size=7))

    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="middle",
            y=-0.1,
            xanchor="center",
            x=0.5,
            font=dict(size=14),
        ),
        polar=dict(
            hole=0.1, radialaxis=dict(showticklabels=False, ticks="", linewidth=0)
        ),
        margin=dict(t=110),
        title=dict(
            text=f"{location}: Wind Distribution", xanchor="center", yanchor="top"
        ),
    )
    fig.show()


def find_closest_cardinal_direction(degree: float):
    # Normalize the degree value to be between 0 and 360
    degree = degree % 360

    # Define the cardinal and intermediate directions and their corresponding degrees
    directions = {
        "North": 0,
        "NNE": 22.5,
        "NE": 45,
        "ENE": 67.5,
        "East": 90,
        "ESE": 112.5,
        "SE": 135,
        "SSE": 157.5,
        "South": 180,
        "SSW": 202.5,
        "SW": 225,
        "WSW": 247.5,
        "West": 270,
        "WNW": 292.5,
        "NW": 315,
        "NNW": 337.5,
    }

    # Initialize variables to keep track of the closest direction and its degree difference
    closest_direction = None
    min_difference = float("inf")

    # Iterate over the directions and calculate the difference in degrees
    for direction, direction_degree in directions.items():
        difference = abs(degree - direction_degree)

        # Check if the current difference is smaller than the previous minimum difference
        if difference < min_difference:
            min_difference = difference
            closest_direction = direction

    return directions[closest_direction]
