"""
Tests for config.py - Configuration loading functionality.

This module tests the simple configuration loading that:
1. Loads YAML configuration files
2. Creates simple config dictionaries and DotConfig objects
3. Provides access to catalog, schema, and processing settings
"""

import pytest
from pathlib import Path


def test_config_loading():
    """Test basic config loading from YAML returns dictionary."""

    from petrinex.config import load_config

    # Test loading the actual config file
    try:
        config = load_config("src/config.yaml")

        # Test basic structure - should be a dictionary
        assert isinstance(config, dict), "Config should be a dictionary"
        assert "catalog" in config, "Config should have catalog"
        assert "schema" in config, "Config should have schema"

        # Test catalog and schema are strings
        assert isinstance(config["catalog"], str), "Catalog should be string"
        assert isinstance(config["schema"], str), "Schema should be string"

        assert "download_dir" in config, "Ingest should have download_dir"
        assert "start_month_yyyy_mm" in config, "Ingest should have start_month"
        assert "conventional" in config, "Ingest should have conventional dataset"
        assert "ngl" in config, "Ingest should have ngl dataset"

        # Test dataset configurations
        conv = config["conventional"]
        assert "code" in conv, "Conventional should have code"
        assert "name" in conv, "Conventional should have name"

        ngl = config["ngl"]
        assert "code" in ngl, "NGL should have code"
        assert "name" in ngl, "NGL should have name"

    except FileNotFoundError:
        pytest.skip("Config file not available for testing")
    except Exception as e:
        pytest.fail(f"Config loading failed: {e}")


def test_dot_config():
    """Test DotConfig class provides dot notation access."""

    from petrinex.config import DotConfig

    try:
        config = DotConfig("src/config.yaml")

        # Test dot notation access
        assert config.catalog, "Should access catalog via dot notation"
        assert config.schema, "Should access schema via dot notation"
        assert config.download_dir, "Should access nested values via dot notation"
        assert config.conventional.code, "Should access deeply nested values"

        # Test that it still behaves like a dict
        assert isinstance(config, dict), "DotConfig should still be a dictionary"
        assert config["catalog"] == config.catalog, "Dict and dot access should match"

    except FileNotFoundError:
        pytest.skip("Config file not available for testing")


def test_config_dataset_properties():
    """Test dataset configuration properties."""

    from petrinex.config import load_config

    try:
        config = load_config("src/config.yaml")

        # Test conventional dataset
        conv = config["conventional"]
        assert conv["code"] in ["Vol", "vol"], "Conventional code should be Vol/vol"
        assert (
            "conventional" in conv["name"].lower()
            or "volumetric" in conv["name"].lower()
        )
        assert conv["download"] in [True, False], "Download should be boolean"

        # Test NGL dataset
        ngl = config["ngl"]
        assert ngl["code"] in ["NGL", "ngl"], "NGL code should be NGL/ngl"
        assert "ngl" in ngl["name"].lower()
        assert ngl["download"] in [True, False], "Download should be boolean"

    except FileNotFoundError:
        pytest.skip("Config file not available for testing")


def test_config_required_fields():
    """Test that all required configuration fields are present."""

    from petrinex.config import load_config

    try:
        config = load_config("src/config.yaml")

        # Required top-level fields
        required_fields = [
            "catalog",
            "schema",
            "download_dir",
            "start_month_yyyy_mm",
            "conventional",
            "ngl",
        ]
        for field in required_fields:
            assert field in config, f"Missing required field: {field}"
            assert config[field] is not None, f"Field {field} cannot be None"

        # Required dataset fields
        dataset_fields = ["code", "name", "table_name"]
        for dataset in [config["conventional"], config["ngl"]]:
            for field in dataset_fields:
                assert field in dataset, f"Missing dataset field: {field}"
                assert (
                    dataset[field] is not None
                ), f"Dataset field {field} cannot be None"

    except FileNotFoundError:
        pytest.skip("Config file not available for testing")
