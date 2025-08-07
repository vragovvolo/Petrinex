"""
Tests for config.py - Configuration loading functionality.

This module tests the simple configuration loading that:
1. Loads YAML configuration files
2. Creates simple config objects with proper attributes
3. Provides access to catalog, schema, and processing settings
"""

import pytest
from pathlib import Path


def test_config_loading():
    """Test basic config loading from YAML."""

    from petrinex.config import load_config

    # Test loading the actual config file
    try:
        config = load_config("src/config.yaml")

        # Test basic structure
        assert hasattr(config, "catalog"), "Config should have catalog"
        assert hasattr(config, "schema"), "Config should have schema"
        assert hasattr(config, "ingest"), "Config should have ingest settings"
        assert hasattr(config, "forecast"), "Config should have forecast settings"

        # Test catalog and schema are strings
        assert isinstance(config.catalog, str), "Catalog should be string"
        assert isinstance(config.schema, str), "Schema should be string"

        # Test ingest configuration
        assert hasattr(config.ingest, "download_dir"), "Ingest should have download_dir"
        assert hasattr(
            config.ingest, "start_month_yyyy_mm"
        ), "Ingest should have start_month"
        assert hasattr(
            config.ingest, "conventional"
        ), "Ingest should have conventional dataset"
        assert hasattr(config.ingest, "ngl"), "Ingest should have ngl dataset"

        # Test dataset configurations
        assert hasattr(
            config.ingest.conventional, "code"
        ), "Conventional should have code"
        assert hasattr(
            config.ingest.conventional, "name"
        ), "Conventional should have name"
        assert hasattr(config.ingest.ngl, "code"), "NGL should have code"
        assert hasattr(config.ingest.ngl, "name"), "NGL should have name"

    except FileNotFoundError:
        pytest.skip("Config file not available for testing")
    except Exception as e:
        pytest.fail(f"Config loading failed: {e}")


def test_config_dataset_properties():
    """Test dataset configuration properties."""

    from petrinex.config import load_config

    try:
        config = load_config("src/config.yaml")

        # Test conventional dataset
        conv = config.ingest.conventional
        assert conv.code in ["Vol", "vol"], "Conventional code should be Vol/vol"
        assert "conventional" in conv.name.lower() or "volumetric" in conv.name.lower()
        assert conv.download in [True, False], "Download should be boolean"

        # Test NGL dataset
        ngl = config.ingest.ngl
        assert ngl.code in ["NGL", "ngl"], "NGL code should be NGL/ngl"
        assert "ngl" in ngl.name.lower()
        assert ngl.download in [True, False], "Download should be boolean"

    except FileNotFoundError:
        pytest.skip("Config file not available for testing")


def test_config_required_fields():
    """Test that all required configuration fields are present."""

    from petrinex.config import load_config

    try:
        config = load_config("src/config.yaml")

        # Required top-level fields
        required_fields = ["catalog", "schema", "ingest"]
        for field in required_fields:
            assert hasattr(config, field), f"Missing required field: {field}"
            assert getattr(config, field) is not None, f"Field {field} cannot be None"

        # Required ingest fields
        ingest_fields = ["download_dir", "start_month_yyyy_mm", "conventional", "ngl"]
        for field in ingest_fields:
            assert hasattr(config.ingest, field), f"Missing ingest field: {field}"
            assert (
                getattr(config.ingest, field) is not None
            ), f"Ingest field {field} cannot be None"

        # Required dataset fields
        dataset_fields = ["code", "name", "table_name"]
        for dataset in [config.ingest.conventional, config.ingest.ngl]:
            for field in dataset_fields:
                assert hasattr(dataset, field), f"Missing dataset field: {field}"
                assert (
                    getattr(dataset, field) is not None
                ), f"Dataset field {field} cannot be None"

    except FileNotFoundError:
        pytest.skip("Config file not available for testing")


def test_config_with_minimal_yaml():
    """Test config loading with minimal YAML structure."""

    import tempfile
    import yaml
    from petrinex.config import load_config

    # Create minimal test config
    test_config = {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "ingest": {
            "download_dir": "/tmp/test",
            "start_month_yyyy_mm": "2024-01",
            "conventional": {
                "code": "Vol",
                "name": "Test Conventional",
                "table_name": "TestConventional",
            },
            "ngl": {"code": "NGL", "name": "Test NGL", "table_name": "TestNGL"},
        },
    }

    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(test_config, f)
        temp_path = f.name

    try:
        # Load the test config
        config = load_config(temp_path)

        # Validate loaded config
        assert config.catalog == "test_catalog"
        assert config.schema == "test_schema"
        assert config.ingest.download_dir == "/tmp/test"
        assert config.ingest.start_month_yyyy_mm == "2024-01"
        assert config.ingest.conventional.code == "Vol"
        assert config.ingest.ngl.code == "NGL"

    finally:
        # Cleanup
        Path(temp_path).unlink()


def test_config_defaults():
    """Test that config applies reasonable defaults."""

    import tempfile
    import yaml
    from petrinex.config import load_config

    # Config with minimal required fields
    minimal_config = {
        "catalog": "test",
        "schema": "test",
        "ingest": {
            "download_dir": "/tmp",
            "start_month_yyyy_mm": "2024-01",
            "conventional": {"code": "Vol", "name": "Test", "table_name": "Test"},
            "ngl": {"code": "NGL", "name": "Test", "table_name": "Test"},
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(minimal_config, f)
        temp_path = f.name

    try:
        config = load_config(temp_path)

        # Test defaults are applied
        assert hasattr(config.ingest, "timeout_seconds"), "Should have timeout default"
        assert (
            config.ingest.timeout_seconds == 30
        ), "Default timeout should be 30 seconds"

        assert hasattr(
            config.ingest.conventional, "download"
        ), "Should have download default"
        assert (
            config.ingest.conventional.download is True
        ), "Default download should be True"

        assert hasattr(config.ingest.ngl, "download"), "Should have download default"
        assert config.ingest.ngl.download is True, "Default download should be True"

    finally:
        Path(temp_path).unlink()
