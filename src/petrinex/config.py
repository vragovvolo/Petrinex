import yaml
from pathlib import Path


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, "r") as f:
        return yaml.safe_load(f)


class DotDict(dict):
    def __getattr__(self, name):
        try:
            value = self[name]

            if isinstance(value, dict):
                return DotDict(value)
            return value
        except KeyError:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )


class DotConfig(DotDict):
    def __init__(self, config_path: str = "config.yaml"):
        config_data = load_config(config_path)
        super().__init__(config_data)
