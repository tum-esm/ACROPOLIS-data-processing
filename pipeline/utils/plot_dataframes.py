import numpy as np
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.express as px
import math
from typing import Optional, Any


def plot_column(df: pl.DataFrame,
                datetime_col: str,
                col1: str,
                sample_size: int = 10000,
                filter_value: int = 1000) -> None:
    """
    Plots the column over a datetime index.
    
    Parameters:
    - df (pl.DataFrame): The Polars DataFrame containing the columns.
    - datetime_col (str): The column name for datetime values.
    - col1 (str): The first column name.
    - sample_size (int): Number of points to sample for plotting (default=10,000).
    - filter_value (int): Maximum value to filter out (default=1,000).
    """
    # Ensure columns exist
    for col in [datetime_col, col1]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")

    # Convert datetime column to proper format if needed
    if not isinstance(df[datetime_col].dtype, pl.Datetime):
        df = df.with_columns(
            pl.col(datetime_col).cast(pl.Datetime).alias(datetime_col))

    # Apply filter if present
    df = df.filter(pl.col(col1) < filter_value) \
        .filter(pl.col(col1) > -filter_value)

    # Downsampling for large data
    num_rows = df.height
    if num_rows > sample_size:
        indices = np.linspace(0, num_rows - 1, sample_size, dtype=int)
        df_sampled = df[indices]
    else:
        df_sampled = df

    # Convert datetime to Python datetime for Matplotlib compatibility
    x_values = df_sampled[datetime_col].to_numpy()
    y_values = df_sampled[col1].to_numpy()

    # Plot the difference with datetime on x-axis
    plt.figure(figsize=(12, 5))
    plt.plot(x_values, y_values, label=f"{col1}", alpha=0.7, linewidth=1)

    # Format the x-axis for datetime
    plt.xlabel("Datetime")
    plt.ylabel(col1)
    plt.legend()
    plt.grid(True)

    # Format date ticks
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)

    plt.show()


def plot_column_difference(df: pl.DataFrame,
                           datetime_col: str,
                           col1: str,
                           col2: str,
                           sample_size: int = 10000,
                           filter_value: int = 1000) -> None:
    """
    Plots the difference between two columns over a datetime index.
    
    Parameters:
    - df (pl.DataFrame): The Polars DataFrame containing the columns.
    - datetime_col (str): The column name for datetime values.
    - col1 (str): The first column name.
    - col2 (str): The second column name.
    - sample_size (int): Number of points to sample for plotting (default=10,000).
    filter_value (int): Maximum value to filter out (default=1000).
    """
    # Ensure columns exist
    for col in [datetime_col, col1, col2]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")

    # Convert datetime column to proper format if needed
    if not isinstance(df[datetime_col].dtype, pl.Datetime):
        df = df.with_columns(
            pl.col(datetime_col).cast(pl.Datetime).alias(datetime_col))

    # Compute the difference & apply filter
    df = df.with_columns((pl.col(col1) - pl.col(col2)).alias("difference")) \
        .filter(pl.col("difference") < filter_value) \
        .filter(pl.col("difference") > -filter_value)

    # Downsampling for large data
    num_rows = df.height
    if num_rows > sample_size:
        indices = np.linspace(0, num_rows - 1, sample_size, dtype=int)
        df_sampled = df[indices]
    else:
        df_sampled = df

    # Convert datetime to Python datetime for Matplotlib compatibility
    x_values = df_sampled[datetime_col].to_numpy()
    y_values = df_sampled["difference"].to_numpy()

    # Plot the difference with datetime on x-axis
    plt.figure(figsize=(12, 5))
    plt.plot(x_values,
             y_values,
             label=f"{col1} - {col2}",
             alpha=0.7,
             linewidth=1)

    # Format the x-axis for datetime
    plt.xlabel("Datetime")
    plt.ylabel("Difference")
    plt.title(f"Difference Between {col1} and {col2} Over Time")
    plt.legend()
    plt.grid(True)

    # Format date ticks
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)

    plt.show()


def plot_sensor_measurement(df,
                            sensor_id,
                            col_name: str,
                            cut_below: Optional[int] = None,
                            cut_above: Optional[int] = None) -> None:
    df = df.select("datetime", "system_id", col_name) \
        .sort("system_id") \
        .filter(pl.col("system_id").is_in(sensor_id)) \
        .filter(pl.col(col_name).is_not_nan())

    if cut_below:
        df = df.filter(pl.col(col_name) < cut_below)
    if cut_above:
        df = df.filter(pl.col(col_name) > -cut_above)

    fig = px.line(
        df,
        x="datetime",
        y=col_name,
        markers=True,
        title=col_name,
        color="system_id",
    )
    fig.show()


def plot_wind_rose(df, id: int, location: str) -> None:
    df_w = df.clone()
    # filter for system
    df_w = df_w.filter(pl.col("system_id") == id).filter(
        pl.col("wxt532_direction_avg") > 0)
    # create bins for wind direction
    df_w = df_w.with_columns(
        pl.col("wxt532_direction_avg").map_elements(
            find_closest_cardinal_direction,
            return_dtype=pl.Float32).alias("cardinal_direction"))
    # create bins for wind speed
    df_w = df_w.with_columns(
        pl.col("wxt532_speed_avg").map_elements(
            lambda t: math.ceil(t * 2) / 2,
            return_dtype=pl.Float32).alias("strength"))
    # groupby relevant bins
    df_w = df_w.group_by(["cardinal_direction",
                          "strength"]).count().sort("strength")

    fig = px.bar_polar(
        df_w,
        r="count",
        theta="cardinal_direction",
        color="strength",
        template="seaborn",
    )

    fig.add_annotation(text="Calm",
                       x=0.5,
                       y=0.5,
                       showarrow=False,
                       font=dict(size=7))

    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="middle",
            y=-0.1,
            xanchor="center",
            x=0.5,
            font=dict(size=14),
        ),
        polar=dict(hole=0.1,
                   radialaxis=dict(showticklabels=False, ticks="",
                                   linewidth=0)),
        margin=dict(t=110),
        title=dict(text=f"{location}: Wind Distribution",
                   xanchor="center",
                   yanchor="top"),
    )
    fig.show()


def plot_co2_rose(df, id: int, location: str) -> None:
    df_w = df.clone()
    # filter for system
    df_w = df_w.filter(pl.col("system_id") == id).filter(
        pl.col("wxt532_direction_avg") > 0)
    # create bins for wind direction
    df_w = df_w.with_columns(
        pl.col("wxt532_direction_avg").map_elements(
            find_closest_cardinal_direction,
            return_dtype=pl.Float32).alias("cardinal_direction"))
    # create bins for wind speed
    df_w = df_w.with_columns(
        pl.col("gmp343_corrected").map_elements(
            lambda t: math.ceil(t * 2) / 2,
            return_dtype=pl.Float32).alias("CO2 concentration (ppm)"))
    # groupby relevant bins
    df_w = (df_w.group_by(["cardinal_direction", "CO2 concentration (ppm)"
                           ]).count().sort("CO2 concentration (ppm)"))

    fig = px.bar_polar(
        df_w,
        r="count",
        theta="cardinal_direction",
        color="CO2 concentration (ppm)",
        template="seaborn",
    )

    fig.add_annotation(text="Calm",
                       x=0.5,
                       y=0.5,
                       showarrow=False,
                       font=dict(size=7))

    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="middle",
            y=-0.1,
            xanchor="center",
            x=0.5,
            font=dict(size=14),
        ),
        polar=dict(hole=0.1,
                   radialaxis=dict(showticklabels=False, ticks="",
                                   linewidth=0)),
        margin=dict(t=110),
        title=dict(text=f"{location}: Wind Distribution",
                   xanchor="center",
                   yanchor="top"),
    )
    fig.show()


def find_closest_cardinal_direction(degree: float) -> float:
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
    min_difference = float("inf")

    # Iterate over the directions and calculate the difference in degrees
    for direction, direction_degree in directions.items():
        difference = abs(degree - direction_degree)

        # Check if the current difference is smaller than the previous minimum difference
        if difference < min_difference:
            min_difference = difference
            closest_direction = direction

    return directions[closest_direction]
