import yaml
from pathlib import Path


def load_config():
    config_path = Path("configs/settings.yaml")

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    return config