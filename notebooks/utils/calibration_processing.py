import warnings
import os
import polars as pl
import numpy

DATA_DIRECTORY = os.environ.get("DATA_DIRECTORY")

warnings.simplefilter("ignore", category=FutureWarning)

# load calibration bottle concentrations (preprocessed)
df_gas = pl.read_csv(
    os.path.join(DATA_DIRECTORY, "input", "averaged_gases.csv"))

# define functions


def process_bottle(data: list, ignore_len: bool = False):
    if ignore_len:
        x = data[int(len(data) * 0.3):int(len(data) * 0.95)]
        return numpy.median(x)
    # 2nd bottle
    if 50 < len(data) < 70:
        x = data[int(len(data) * 0.3):int(len(data) * 0.95)]
        return numpy.median(x)
    # 1st bottle
    elif 70 < len(data) < 130:
        x = data[int(len(data) * 0.5):int(len(data) * 0.95)]
        return numpy.median(x)
    else:
        return 0.0


def process_bottle_mean(data: list, ignore_len: bool = False):
    if ignore_len:
        x = data[int(len(data) * 0.3):int(len(data) * 0.95)]
        return sum(x) / len(x)
    # 2nd bottle
    if 50 < len(data) < 70:
        x = data[int(len(data) * 0.3):int(len(data) * 0.95)]
        return sum(x) / len(x)
    # 1st bottle
    elif 70 < len(data) < 130:
        x = data[int(len(data) * 0.5):int(len(data) * 0.95)]
        return sum(x) / len(x)
    else:
        return 0.0


def two_point_calibration(measured_values: list, true_values: list):
    # Check if input lists have length 2
    if len(measured_values) != 2 or len(true_values) != 2:
        return 0, 0

    # Calculate calibration parameters (slope and intercept)

    slope = (true_values[1] - true_values[0]) / (measured_values[1] -
                                                 measured_values[0])
    # y_true = m * y_meas + t
    intercept = true_values[0] - slope * measured_values[0]

    return {"slope": slope, "intercept": intercept}
