import os

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Evnironment variables
DATA_DIRECTORY = os.environ.get("DATA_DIRECTORY")
PICARRO_DATA_DIRECTORY = os.environ.get("PICARRO_DATA_DIRECTORY")
THINGSBOARD_DATA_DIRECTORY = os.environ.get("THINGSBOARD_DATA_DIRECTORY")

# Input
AVERAGED_GASES = os.path.join(DATA_DIRECTORY, "input", "averaged_gases.csv")
CONFIG_DIRECTORY = os.path.join(PROJECT_DIR, "config")

#Output
PIPELINE_OUTPUT_DIRECTORY = os.path.join(DATA_DIRECTORY, "output", "pipeline")
ICOS_CITIES_LEVEL_1 = os.path.join(DATA_DIRECTORY, "output",
                                   "icos_cities_portal", "level_1")
ICOS_CITIES_LEVEL_2 = os.path.join(DATA_DIRECTORY, "output",
                                   "icos_cities_portal", "level_2")
