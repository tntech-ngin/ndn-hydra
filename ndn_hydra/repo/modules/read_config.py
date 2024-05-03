import os
import yaml


def read_config_file():
    default_config = {}

    script_dir = os.path.dirname(__file__)
    config_path = os.path.join(script_dir, '../config.yaml')
    try:
        with open(config_path, "r") as yamlfile:
            default_config = yaml.safe_load(yamlfile)

    except Exception as e:
        print(f"\n > An error occurred while reading config file: {e}")

    return default_config
