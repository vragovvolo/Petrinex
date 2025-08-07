"""Simple configuration for Petrinex processing."""


def load_config(config_path: str = "src/config.yaml"):
    """Load config from YAML file."""
    import yaml

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Create simple config object with attributes
    class SimpleConfig:
        def __init__(self, data):
            self.catalog = data["catalog"]
            self.schema = data["schema"]
            self.ingest = SimpleIngest(data["ingest"])
            self.forecast = SimpleForecast(data.get("forecast", {}))

    class SimpleIngest:
        def __init__(self, data):
            self.download_dir = data["download_dir"]
            self.start_month_yyyy_mm = data["start_month_yyyy_mm"]
            self.end_month_yyyy_mm = data.get("end_month_yyyy_mm")
            self.timeout_seconds = data.get("timeout_seconds", 30)
            self.conventional = SimpleDataset(data["conventional"])
            self.ngl = SimpleDataset(data["ngl"])

    class SimpleDataset:
        def __init__(self, data):
            self.download = data.get("download", True)
            self.code = data["code"]
            self.name = data["name"]
            self.table_name = data["table_name"]

    class SimpleForecast:
        def __init__(self, data):
            self.model = data.get("model", {})

    return SimpleConfig(config)
