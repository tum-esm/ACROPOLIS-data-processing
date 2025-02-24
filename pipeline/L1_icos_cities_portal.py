import polars as pl
import os
import gc
import time
from datetime import datetime, timedelta
import csv

from utils.config_files import load_json_config
from utils.import_data import import_acropolis_site_data
from utils.datetime_conversions import calculate_decimal_year
from config.sites_deloyment_times import deployment_times

from utils.paths import DESPIKED_DATA_DIRECTORY, CONFIG_DIRECTORY, ICOS_CITIES_LEVEL_1

config = load_json_config("config.json")

path = os.path.join(CONFIG_DIRECTORY, "sites.csv")
sites_meta = pl.read_csv(path, separator=";").with_columns(
    pl.exclude(pl.Utf8).cast(str))

# Record start time
start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script started at: {start_datetime}")

for site in config["icos_cities_portal"]["site_names"]:
    pass

site = "TUMR"

df = import_acropolis_site_data(target_directory=DESPIKED_DATA_DIRECTORY,
                                deployment_times=deployment_times,
                                site_name=site)

# TODO move to utils
df = df.fill_null('') \
        .rename({"gmp343_corrected": "co2", "h2o_v%": "h2o", "enclosure_bme280_pressure":"pressure", "gmp343_temperature":"sensor_temperature", "wxt532_speed_avg": "ws", "wxt532_direction_avg":"wd"}) \
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

# TODO move to utils
# construct icos cities portal head
data_level = 1
header_lines = 45
file_name = f"{site}_munich_acropolis_L{data_level}_1min.csv"
file_path = os.path.join(ICOS_CITIES_LEVEL_1, file_name)
file_lines = len(df) + header_lines
site_short_name = site[:4]
site_long_name = sites_meta.filter(
    pl.col("site") == site[:4]).select("site_name").item()
latitude = sites_meta.filter(
    pl.col("site") == site[:4]).select("latitude").item()
longitude = sites_meta.filter(
    pl.col("site") == site[:4]).select("longitude").item()
altitude = sites_meta.filter(
    pl.col("site") == site[:4]).select("elevation").item()
sampling_height = sites_meta.filter(
    pl.col("site") == site[:4]).select("height_of_building").item()
start_date = df.select("#Datetime").row(0)[0]
stop_date = df.select("#Datetime").row(-1)[0]

with open(file_path, 'w', newline='') as file:
    writer = csv.writer(file, delimiter=';', lineterminator='\n')
    field = [
        "#Datetime", "Year", "Month", "Day", "Hour", "Minute", "Second",
        "DecimalDate", "co2", "h2o", "pressure", "sensor_temperature", "ws",
        "wd", "Flag"
    ]

    writer.writerow([
        "# TITLE: co2 - continuous time series from low and mid cost sensors"
    ])
    writer.writerow([f"# FILE NAME: {file_name}"])
    writer.writerow([
        "# DATA FORMAT: see the last line of this header for column description"
    ])
    writer.writerow([f'# TOTAL LINES: {file_lines}'])
    writer.writerow([f'# HEADER LINES: {header_lines}'])
    writer.writerow(['# PROJECT: ICOS CITIES'])
    writer.writerow([f'# DATA VERSION: L{data_level}'])
    writer.writerow([f'# STATION CODE: {site_short_name}'])
    writer.writerow([f'# STATION NAME: {site_long_name} ({site_short_name})'])
    writer.writerow([
        '# OBSERVATION CATEGORY: Air sampling observation at a stationary platform'
    ])
    writer.writerow(['# COUNTRY/TERRITORY: DE'])
    writer.writerow(
        [r'# RESPONSIBLE INSTITUTE: TUM, Technial University Munich'])
    writer.writerow(
        ['# CONTRIBUTOR:  Patrick Aigner, Jia Chen, Klaus Kürzinger'])
    writer.writerow([
        '# CONTACT POINT: Patrick Aigner <patrick.aigner@tum.de>, Jia Chen <jia.chen@tum.de>'
    ])
    writer.writerow([
        "# FUNDING: European Union's Horizon 2020 Research and Innovation Programme, Grant Agreement No. 101037319"
    ])
    writer.writerow([f'# LATITUDE: {latitude}'])
    writer.writerow([f'# LONGITUDE: {longitude}'])
    writer.writerow([f'# ALTITUDE: {altitude} m asl'])

    if site[:4] == "BLUT":
        writer.writerow([f'# SAMPLING HEIGHTS: {site[-2:]} m agl'])
    else:
        writer.writerow([f'# SAMPLING HEIGHTS: {sampling_height} m agl'])

    writer.writerow(['# PARAMETER: co2'])
    writer.writerow([f'# COVERING PERIOD: {start_date} - {stop_date}'])
    writer.writerow(['# TIME INTERVAL: 1 minute'])
    writer.writerow(['# MEASUREMENT UNIT: µmol/mol'])
    writer.writerow(['# MEASUREMENT METHOD: NDIR'])
    writer.writerow(['# INSTRUMENT: Vaisala GMP343'])
    writer.writerow(['# SAMPLING TYPE: continuous'])
    writer.writerow([
        '# TIME ZONE: Central European Time (UTC+1), Central European Summer Time (UTC+2)'
    ])
    writer.writerow(['# MEASUREMENT SCALE: WMO-CO2-X2019'])
    writer.writerow([
        '# DATA POLICY: ICOS CITIES DATA is licensed under a Creative Commons Attribution 4.0 international licence (http://creativecommons.org/licenses/by/4.0/.The ICOS CITIES data licence is described at https://data.icos-cities.eu/licence.'
    ])
    writer.writerow(['# COMMENT:'])
    writer.writerow(['#'])
    writer.writerow(['#   - Times are UTC+0'])
    writer.writerow([
        '#   - Time-averaged values are reported at the middle of the averaging interval.'
    ])
    writer.writerow(['#   - co2: dry mole air fraction (µmol/mol)'])
    writer.writerow(['#   - h20: absolute humidity in vol%'])
    writer.writerow(
        ['#   - pressure: ambient pressure of outdoor enclosure in hPa'])
    writer.writerow(
        ['#   - sensor_temperature: measurement chamber temperature in °C'])
    writer.writerow(['#   - ws: wind speed in m/s'])
    writer.writerow(['#   - wd: wind direction in degrees'])
    writer.writerow(
        ["#   - Flag 'U' = data correct before manual quality control"])
    writer.writerow([
        "#   - Flag 'H' = Potentially locally contaminated by hampel filter (auto)"
    ])
    writer.writerow([
        '#   - In case of gaps between instruments, the timeseries are filled with empty string'
    ])
    writer.writerow(['#   - Release notes: '])
    writer.writerow(['#'])
    writer.writerow(field)

    for row in df.iter_rows():
        writer.writerow([''.join(item) for item in row])

# Clear memory
del df
gc.collect()  # Explicitly run garbage collection

# Record end time
end_time = time.time()
duration = end_time - start_time
end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"Script ended at: {end_datetime}")
print(f"Total duration: {duration:.2f} seconds")
