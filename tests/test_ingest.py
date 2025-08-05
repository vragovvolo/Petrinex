from petrinex.ingest import _generate_month_list
from petrinex.config import IngestConfig, DatasetConfig, PetrinexConfig

class TestMonthListGeneration:
    """Test month list generation - pure function, no dependencies"""

    def test_single_month(self):
        months = _generate_month_list("2024-01", "2024-01")
        assert months == ["2024-01"]

    def test_multiple_months(self):
        months = _generate_month_list("2024-01", "2024-03")
        assert months == ["2024-01", "2024-02", "2024-03"]

    def test_year_boundary(self):
        months = _generate_month_list("2023-11", "2024-02")
        assert months == ["2023-11", "2023-12", "2024-01", "2024-02"]


class TestDatasetConfig:
    """Test dataset configuration behavior"""

    def test_download_subdir_property(self):
        """Test that subdirectory names are generated correctly"""
        conventional = DatasetConfig(
            download=True, code="Vol", name="Conventional", table_name="Test"
        )
        ngl = DatasetConfig(download=True, code="NGL", name="NGL", table_name="Test")

        assert conventional.download_subdir == "vol"
        assert ngl.download_subdir == "ngl"

    def test_download_flag_behavior(self):
        """Test download flag defaults and assignment"""
        # Default should be True
        dataset1 = DatasetConfig(code="Vol", name="Test", table_name="Test")
        assert dataset1.download is True

        # Can be set to False
        dataset2 = DatasetConfig(
            download=False, code="Vol", name="Test", table_name="Test"
        )
        assert dataset2.download is False


class TestIngestConfig:
    """Test ingest configuration logic"""

    def test_effective_end_month_with_explicit_value(self):
        """Test effective_end_month when end_month is specified"""
        config = IngestConfig(
            start_month_yyyy_mm="2024-01", end_month_yyyy_mm="2024-03"
        )
        assert config.effective_end_month == "2024-03"

    def test_effective_end_month_current_fallback(self):
        """Test effective_end_month defaults to current month format"""
        config = IngestConfig(start_month_yyyy_mm="2024-01")
        # Just verify it's in the right format, not the exact value
        import re

        assert re.match(r"\d{4}-\d{2}", config.effective_end_month)

    def test_dataset_download_settings(self):
        """Test that dataset download settings work correctly"""
        config = IngestConfig(
            start_month_yyyy_mm="2024-01",
            conventional=DatasetConfig(
                download=True,
                code="Vol",
                name="Conventional",
                table_name="ConventionalVolumetric",
            ),
            ngl=DatasetConfig(
                download=False, code="NGL", name="NGL", table_name="NGLVolumetric"
            ),
        )

        assert config.conventional.download is True
        assert config.ngl.download is False
        assert config.conventional.code == "Vol"
        assert config.ngl.code == "NGL"


class TestPathConstruction:
    """Test path and URL construction logic"""

    def test_api_url_construction(self):
        """Test API URL is built correctly"""
        config = IngestConfig(start_month_yyyy_mm="2024-01")
        dataset = config.conventional
        month = "2024-01"

        expected_url = f"{config.api_base_url}/{dataset.code}/{month}/CSV"
        assert (
            expected_url
            == "https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2024-01/CSV"
        )

    def test_download_path_logic(self):
        """Test download path construction logic"""
        config = IngestConfig(start_month_yyyy_mm="2024-01", download_dir="/tmp/test")

        # Test the logic that would be used in actual path construction
        conventional_subdir = config.conventional.download_subdir
        ngl_subdir = config.ngl.download_subdir

        assert conventional_subdir == "vol"
        assert ngl_subdir == "ngl"

        # Verify path joining logic (what the real code would do)
        import os

        conventional_path = os.path.join(config.download_dir, conventional_subdir)
        ngl_path = os.path.join(config.download_dir, ngl_subdir)

        assert conventional_path == "/tmp/test/vol"
        assert ngl_path == "/tmp/test/ngl"


class TestFullTableNames:
    """Test table name construction"""

    def test_table_name_construction(self):
        """Test full table names are built correctly"""
        config = PetrinexConfig(
            catalog="test_catalog",
            schema="test_schema",
            ingest=IngestConfig(start_month_yyyy_mm="2024-01"),
            process={
                "conventional_table_name": "ConventionalVolumetric",
                "ngl_table_name": "NGLVolumetric",
                "wells_table_name": "Well",
            },
            forecast={"model": {"name": "test", "version": "1.0", "type": "test"}},
        )

        assert (
            config.full_conventional_table_name
            == "test_catalog.test_schema.ConventionalVolumetric"
        )
        assert config.full_ngl_table_name == "test_catalog.test_schema.NGLVolumetric"
        assert config.full_wells_table_name == "test_catalog.test_schema.Well"
