from typing import Optional, List, Any
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class ModelParameter(BaseModel):
    """Model parameter configuration"""

    name: str
    value: Any


class ForecastModel(BaseModel):
    """Forecast model configuration"""

    name: str
    version: str
    type: str
    parameters: List[ModelParameter] = []


class ForecastConfig(BaseModel):
    """Forecast configuration"""

    model: ForecastModel


class DatasetConfig(BaseModel):
    """Configuration for a specific dataset"""

    download: bool = Field(True, description="Whether to download this dataset")
    code: str = Field(..., description="Dataset code for API")
    name: str = Field(..., description="Human readable dataset name")
    table_name: str = Field(..., description="Target table name")

    @property
    def download_subdir(self) -> str:
        """Get subdirectory name for this dataset"""
        return self.code.lower()


def _get_default_download_dir() -> str:
    """Get default download directory - defaults to Volumes"""
    return "/Volumes/main/default/petrinex_downloads"


class IngestConfig(BaseModel):
    """Ingest configuration"""

    start_month_yyyy_mm: str = Field(..., description="Start month in YYYY-MM format")
    end_month_yyyy_mm: Optional[str] = Field(
        None, description="End month in YYYY-MM format"
    )

    # Download settings
    download_dir: str = Field(
        default_factory=_get_default_download_dir, description="Directory for downloads"
    )
    api_base_url: str = Field(
        "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB",
        description="Base API URL",
    )
    timeout_seconds: int = Field(30, description="HTTP request timeout in seconds")

    # Dataset configurations
    conventional: DatasetConfig = Field(
        default_factory=lambda: DatasetConfig(
            download=True,
            code="Vol",
            name="Conventional Volumetric",
            table_name="ConventionalVolumetric",
        ),
        description="Conventional dataset configuration",
    )

    ngl: DatasetConfig = Field(
        default_factory=lambda: DatasetConfig(
            download=True,
            code="NGL",
            name="NGL & Marketable Gas Volumes",
            table_name="NGLVolumetric",
        ),
        description="NGL dataset configuration",
    )

    @field_validator("start_month_yyyy_mm")
    @classmethod
    def validate_start_month_format(cls, v):
        """Validate start month is in correct YYYY-MM format"""
        # Check exact format first - must be exactly 7 characters
        if len(v) != 7 or v[4] != "-":
            raise ValueError("start_month_yyyy_mm must be in YYYY-MM format")
        try:
            datetime.strptime(v, "%Y-%m")
        except ValueError:
            raise ValueError("start_month_yyyy_mm must be in YYYY-MM format")
        return v

    @field_validator("end_month_yyyy_mm")
    @classmethod
    def validate_end_month_format(cls, v):
        """Validate end month is in correct YYYY-MM format"""
        if v is not None:
            # Check exact format first - must be exactly 7 characters
            if len(v) != 7 or v[4] != "-":
                raise ValueError("end_month_yyyy_mm must be in YYYY-MM format")
            try:
                datetime.strptime(v, "%Y-%m")
            except ValueError:
                raise ValueError("end_month_yyyy_mm must be in YYYY-MM format")
        return v

    @property
    def effective_end_month(self) -> str:
        """Return end month, defaulting to current month if not specified"""
        if self.end_month_yyyy_mm:
            return self.end_month_yyyy_mm
        return datetime.now().strftime("%Y-%m")


class ProcessConfig(BaseModel):
    """Process configuration"""

    conventional_table_name: str = Field(
        "ConventionalVolumetric", description="Name for conventional table"
    )
    ngl_table_name: str = Field("NGLVolumetric", description="Name for NGL table")
    wells_table_name: str = Field("Well", description="Name for wells table")


class PetrinexConfig(BaseModel):
    """Main Petrinex configuration model"""

    catalog: str = Field(..., description="Databricks catalog name")
    schema_name: str = Field(..., alias="schema", description="Databricks schema name")
    ingest: IngestConfig
    process: ProcessConfig
    forecast: ForecastConfig

    @property
    def full_conventional_table_name(self) -> str:
        """Generate full conventional table name"""
        return (
            f"{self.catalog}.{self.schema_name}.{self.ingest.conventional.table_name}"
        )

    @property
    def full_ngl_table_name(self) -> str:
        """Generate full NGL table name"""
        return f"{self.catalog}.{self.schema_name}.{self.ingest.ngl.table_name}"

    @property
    def full_wells_table_name(self) -> str:
        """Generate full wells table name"""
        return f"{self.catalog}.{self.schema_name}.{self.process.wells_table_name}"


def load_config(config_path: str = "config.yaml") -> PetrinexConfig:
    """Load config using MLflow ModelConfig for compatibility with current usage"""
    from mlflow.models import ModelConfig

    config_dict = ModelConfig(development_config=config_path).to_dict()
    return PetrinexConfig(**config_dict)
