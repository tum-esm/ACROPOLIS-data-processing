import os
from utils.config_files import load_json_config

config = load_json_config("config.json")

PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Input Data Directories
PICARRO_DATA_DIRECTORY = config["measurement_data_paths"]["picarro"]

THINGSBOARD_DATA_DIRECTORY = config["measurement_data_paths"]["thingsboard"]

# Project Data Directories
DATA_DIRECTORY = os.path.join(PROJECT_DIR, "data")

AVERAGED_GASES = os.path.join(DATA_DIRECTORY, "input", "averaged_gases.csv")

PROCESSED_PICARRO_DATA_DIRECTORY = os.path.join(DATA_DIRECTORY, "input",
                                                "picarro")

PIPELINE_DATA_DIRECTORY = os.path.join(DATA_DIRECTORY, "output", "pipeline")

POSTPROCESSED_DATA_DIRECTORY = os.path.join(PIPELINE_DATA_DIRECTORY,
                                            "postprocessed")

DESPIKED_DATA_DIRECTORY = os.path.join(PIPELINE_DATA_DIRECTORY, "despiked")

ICOS_CITIES_LEVEL_1 = os.path.join(PIPELINE_DATA_DIRECTORY,
                                   "icos_cities_portal", "level_1")

ICOS_CITIES_LEVEL_2 = os.path.join(PIPELINE_DATA_DIRECTORY,
                                   "icos_cities_portal", "level_2")
