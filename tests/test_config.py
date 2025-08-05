import pytest
import os
from datetime import datetime
from pydantic import ValidationError

from petrinex.config import (
    PetrinexConfig,
    IngestConfig,
    ProcessConfig,
    ForecastConfig,
    ForecastModel,
    ModelParameter,
    DatasetConfig,
    load_config,
)


class TestDatasetConfig:
    """Test DatasetConfig model"""

    def test_dataset_config_creation(self):
        """Test creating a dataset config"""
        config = DatasetConfig(
            download=True,
            code="Vol",
            name="Conventional Volumetric",
            table_name="ConventionalVolumetric",
        )
        assert config.download is True
        assert config.code == "Vol"
        assert config.name == "Conventional Volumetric"
        assert config.table_name == "ConventionalVolumetric"
        assert config.download_subdir == "vol"

    def test_download_subdir_property(self):
        """Test download_subdir property converts to lowercase"""
        config = DatasetConfig(
            download=True, code="NGL", name="Test", table_name="Test"
        )
        assert config.download_subdir == "ngl"

    def test_dataset_config_download_default(self):
        """Test that download defaults to True"""
        config = DatasetConfig(code="Vol", name="Test", table_name="Test")
        assert config.download is True

    def test_dataset_config_download_false(self):
        """Test setting download to False"""
        config = DatasetConfig(
            download=False, code="Vol", name="Test", table_name="Test"
        )
        assert config.download is False


class TestModelParameter:
    """Test ModelParameter model"""

    def test_model_parameter_creation(self):
        """Test creating a model parameter"""
        param = ModelParameter(name="forecast_horizon", value=12)
        assert param.name == "forecast_horizon"
        assert param.value == 12

    def test_model_parameter_with_string_value(self):
        """Test model parameter with string value"""
        param = ModelParameter(name="model_type", value="petrinex")
        assert param.name == "model_type"
        assert param.value == "petrinex"


class TestForecastModel:
    """Test ForecastModel model"""

    def test_forecast_model_creation(self):
        """Test creating a forecast model"""
        param = ModelParameter(name="forecast_horizon", value=12)
        model = ForecastModel(
            name="petrinex", version="1.0.0", type="petrinex", parameters=[param]
        )
        assert model.name == "petrinex"
        assert model.version == "1.0.0"
        assert model.type == "petrinex"
        assert len(model.parameters) == 1

    def test_forecast_model_empty_parameters(self):
        """Test forecast model with empty parameters"""
        model = ForecastModel(name="petrinex", version="1.0.0", type="petrinex")
        assert len(model.parameters) == 0


class TestIngestConfig:
    """Test IngestConfig model"""

    def test_ingest_config_valid(self):
        """Test creating valid ingest config with new structure"""
        config = IngestConfig(
            start_month_yyyy_mm="2024-01", end_month_yyyy_mm="2024-03"
        )
        assert config.start_month_yyyy_mm == "2024-01"
        assert config.end_month_yyyy_mm == "2024-03"
        # Test default dataset configs
        assert config.conventional.download is True
        assert config.conventional.code == "Vol"
        assert config.conventional.table_name == "ConventionalVolumetric"
        assert config.ngl.download is True
        assert config.ngl.code == "NGL"
        assert config.ngl.table_name == "NGLVolumetric"

    def test_ingest_config_defaults(self):
        """Test ingest config with default values"""
        config = IngestConfig(start_month_yyyy_mm="2024-01")
        assert config.end_month_yyyy_mm is None
        # Test that download settings have defaults
        assert config.timeout_seconds == 30
        assert config.download_dir == "/Volumes/main/default/petrinex_downloads"
        assert (
            config.api_base_url
            == "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB"
        )
        # Test default dataset download settings
        assert config.conventional.download is True
        assert config.ngl.download is True

    def test_effective_end_month_with_value(self):
        """Test effective_end_month when end_month is specified"""
        config = IngestConfig(
            start_month_yyyy_mm="2024-01", end_month_yyyy_mm="2024-03"
        )
        assert config.effective_end_month == "2024-03"

    def test_effective_end_month_without_value(self):
        """Test effective_end_month when end_month is None"""
        config = IngestConfig(start_month_yyyy_mm="2024-01")
        current_month = datetime.now().strftime("%Y-%m")
        assert config.effective_end_month == current_month

    def test_invalid_start_month_format(self):
        """Test validation error for invalid start month format"""
        with pytest.raises(ValidationError) as exc_info:
            IngestConfig(start_month_yyyy_mm="2024-1")  # Missing zero padding

        assert "start_month_yyyy_mm must be in YYYY-MM format" in str(exc_info.value)

    def test_invalid_end_month_format(self):
        """Test validation error for invalid end month format"""
        with pytest.raises(ValidationError) as exc_info:
            IngestConfig(
                start_month_yyyy_mm="2024-01",
                end_month_yyyy_mm="24-01",  # Invalid format
            )

        assert "end_month_yyyy_mm must be in YYYY-MM format" in str(exc_info.value)

    def test_valid_month_formats(self):
        """Test various valid month formats"""
        valid_months = ["2024-01", "2023-12", "2025-06"]
        for month in valid_months:
            config = IngestConfig(start_month_yyyy_mm=month)
            assert config.start_month_yyyy_mm == month

    def test_custom_dataset_configs(self):
        """Test ingest config with custom dataset configurations"""
        custom_conventional = DatasetConfig(
            download=False,
            code="CustomVol",
            name="Custom Conventional",
            table_name="CustomConventionalTable",
        )
        custom_ngl = DatasetConfig(
            download=True,
            code="CustomNGL",
            name="Custom NGL",
            table_name="CustomNGLTable",
        )

        config = IngestConfig(
            start_month_yyyy_mm="2024-01",
            conventional=custom_conventional,
            ngl=custom_ngl,
        )

        assert config.conventional.download is False
        assert config.conventional.code == "CustomVol"
        assert config.conventional.table_name == "CustomConventionalTable"
        assert config.ngl.download is True
        assert config.ngl.code == "CustomNGL"
        assert config.ngl.table_name == "CustomNGLTable"

    def test_custom_download_settings(self):
        """Test ingest config with custom download settings"""
        config = IngestConfig(
            start_month_yyyy_mm="2024-01",
            download_dir="/custom/path",
            timeout_seconds=60,
            api_base_url="https://custom.api.com",
        )

        assert config.download_dir == "/custom/path"
        assert config.timeout_seconds == 60
        assert config.api_base_url == "https://custom.api.com"


class TestPetrinexConfig:
    """Test PetrinexConfig model"""

    def test_full_table_names(self):
        """Test generation of full table names with new structure"""
        config = PetrinexConfig(
            catalog="test_catalog",
            schema="test_schema",
            ingest=IngestConfig(start_month_yyyy_mm="2024-01"),
            process=ProcessConfig(),
            forecast=ForecastConfig(
                model=ForecastModel(name="test", version="1.0", type="test")
            ),
        )

        assert (
            config.full_conventional_table_name
            == "test_catalog.test_schema.ConventionalVolumetric"
        )
        assert config.full_ngl_table_name == "test_catalog.test_schema.NGLVolumetric"
        assert config.full_wells_table_name == "test_catalog.test_schema.Well"

    def test_config_with_custom_table_names(self):
        """Test config with custom table names"""
        custom_conventional = DatasetConfig(
            code="Vol", name="Conventional Volumetric", table_name="CustomConventional"
        )
        custom_ngl = DatasetConfig(
            code="NGL", name="NGL Volumetric", table_name="CustomNGL"
        )

        config = PetrinexConfig(
            catalog="prod",
            schema="petrinex",
            ingest=IngestConfig(
                start_month_yyyy_mm="2024-01",
                conventional=custom_conventional,
                ngl=custom_ngl,
            ),
            process=ProcessConfig(wells_table_name="CustomWells"),
            forecast=ForecastConfig(
                model=ForecastModel(name="test", version="1.0", type="test")
            ),
        )

        assert config.full_conventional_table_name == "prod.petrinex.CustomConventional"
        assert config.full_ngl_table_name == "prod.petrinex.CustomNGL"
        assert config.full_wells_table_name == "prod.petrinex.CustomWells"


class TestConfigLoading:
    """Test config loading functions"""

    def test_load_config_from_mlflow(self):
        """Test loading config using MLflow"""
        # This test assumes config.yaml exists and loads via MLflow
        if os.path.exists("config.yaml"):
            config = load_config("config.yaml")
            assert isinstance(config, PetrinexConfig)
            assert config.catalog == "shm"
            assert config.schema_name == "petrinex"
