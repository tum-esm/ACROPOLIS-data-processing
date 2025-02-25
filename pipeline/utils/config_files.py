import os
import json

PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CONFIG_DIRECTORY = os.path.join(PROJECT_DIR, "pipeline", "config")


def load_json_config(file_name: str) -> dict:
    """Load JSON file from config folder."""
    assert (file_name.endswith('.json'))
    config_path = os.path.join(CONFIG_DIRECTORY, file_name)

    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    else:
        raise FileNotFoundError(f"Config file not found: {config_path}")
