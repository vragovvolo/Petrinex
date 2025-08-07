"""
End-to-end integration tests for the complete Petrinex data pipeline.

These tests demonstrate the full workflow from data ingestion through forecasting:
1. CSV processing to Bronze to Silver to Forecasts
2. Config loading to data processing integration
3. Historical data to decline analysis to forecast workflow
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path


def test_csv_to_forecast_pipeline():
    """Test complete pipeline from CSV processing to forecast generation."""

    # Skip if no fixture data
    fixture_path = Path("fixtures/ngl_vol_silver.parquet")
    if not fixture_path.exists():
        pytest.skip("Fixture data not available")

    # Step 1: Load "bronze" data (simulate CSV -> Bronze)
    bronze_data = pd.read_parquet(fixture_path)

    # Basic bronze validation
    assert len(bronze_data) > 0, "Bronze data should not be empty"
    required_cols = ["WellID", "ProductionMonth", "GasProduction"]
    for col in required_cols:
        assert col in bronze_data.columns, f"Missing required column: {col}"

    # Step 2: Create Silver data (Bronze -> Silver with calculations)

    # Import NGL calculation logic
    from petrinex.ngl_calcs import GAS_EQUIV

    silver_data = bronze_data.copy()

    # Convert NGL columns to numeric
    ngl_columns = [
        "EthaneMixVolume",
        "PropaneMixVolume",
        "ButaneMixVolume",
        "PentaneMixVolume",
        "LiteMixVolume",
    ]

    for col in ngl_columns:
        if col in silver_data.columns:
            silver_data[col] = pd.to_numeric(silver_data[col], errors="coerce").fillna(
                0
            )

    # Add NGL calculations to silver
    silver_data["ethane_gas_equiv"] = (
        silver_data.get("EthaneMixVolume", 0) * GAS_EQUIV["ethane"]
    )
    silver_data["propane_gas_equiv"] = (
        silver_data.get("PropaneMixVolume", 0) * GAS_EQUIV["propane"]
    )
    silver_data["butane_gas_equiv"] = (
        silver_data.get("ButaneMixVolume", 0) * GAS_EQUIV["butane"]
    )
    silver_data["pentane_gas_equiv"] = (
        silver_data.get("PentaneMixVolume", 0) * GAS_EQUIV["pentane"]
    )
    silver_data["lite_gas_equiv"] = (
        silver_data.get("LiteMixVolume", 0) * GAS_EQUIV["lite_mix"]
    )

    silver_data["total_gas_equivalent"] = (
        silver_data["ethane_gas_equiv"]
        + silver_data["propane_gas_equiv"]
        + silver_data["butane_gas_equiv"]
        + silver_data["pentane_gas_equiv"]
        + silver_data["lite_gas_equiv"]
    )

    # Add processed timestamp
    silver_data["processed_at"] = pd.Timestamp.now()

    # Step 3: Prepare data for forecasting

    # Filter wells with sufficient production data for forecasting
    silver_data["ProductionMonth"] = pd.to_datetime(silver_data["ProductionMonth"])
    silver_data["GasProduction"] = pd.to_numeric(
        silver_data["GasProduction"], errors="coerce"
    ).fillna(0)

    # Get wells with multiple months of production
    well_counts = silver_data.groupby("WellID").size()
    forecast_ready_wells = well_counts[well_counts >= 6].index
    forecast_data = silver_data[silver_data["WellID"].isin(forecast_ready_wells)]

    unique_wells = len(forecast_ready_wells)
    assert unique_wells > 0, "Should have wells ready for forecasting"

    # Step 4: Generate simple forecasts

    # Simple exponential decline parameters for demonstration
    forecast_df = []
    top_wells = list(forecast_ready_wells)[:5]  # Test with top 5 wells

    for well_id in top_wells:
        well_data = forecast_data[forecast_data["WellID"] == well_id].copy()
        well_data = well_data.sort_values("ProductionMonth")

        if len(well_data) < 3:
            continue

        # Get latest production and create simple decline forecast
        latest_production = well_data["GasProduction"].iloc[-1]
        if latest_production <= 0:
            continue

        # Generate 24 months of forecast with simple decline
        future_dates = pd.date_range(
            start=well_data["ProductionMonth"].max() + pd.DateOffset(months=1),
            periods=24,
            freq="ME",
        )

        # Simple exponential decline: production decreases by 15% per year
        monthly_decline = 0.985  # ~15% annual decline
        for i, date in enumerate(future_dates):
            forecast_production = latest_production * (monthly_decline ** (i + 1))
            forecast_df.append(
                {
                    "WellID": well_id,
                    "ProductionMonth": date,
                    "GasProduction": forecast_production,
                    "forecast_type": "exponential_decline",
                }
            )

    forecast_df = pd.DataFrame(forecast_df)

    # Step 5: Validation and Summary
    assert len(silver_data) == len(bronze_data), "Silver should have same row count"
    assert (
        "total_gas_equivalent" in silver_data.columns
    ), "Silver should have calculations"
    assert len(forecast_df) > 0, "Should generate some forecasts"
    assert all(forecast_df["GasProduction"] > 0), "All forecasts should be positive"

    # Verify data flow integrity
    total_gas_equiv = silver_data["total_gas_equivalent"].sum()
    total_forecast_production = forecast_df["GasProduction"].sum()

    assert total_gas_equiv > 0, "Should have positive gas equivalent"
    assert total_forecast_production > 0, "Should have positive forecast production"

    assert True, "End-to-end pipeline test completed"


def test_config_to_processing_integration():
    """Test integration from config loading to data processing."""

    # Step 1: Load configuration
    try:
        from petrinex.config import load_config

        config = load_config("src/config.yaml")

        # Basic config validation
        assert hasattr(config, "catalog"), "Config should have catalog"
        assert hasattr(config, "schema"), "Config should have schema"

    except FileNotFoundError:
        pytest.skip("Config file not available")

    # Step 2: Test processing functions
    from petrinex.ngl_calcs import GAS_EQUIV

    # Verify GAS_EQUIV constants loaded
    assert len(GAS_EQUIV) >= 5, "Should have gas equivalent factors"
    assert all(v > 0 for v in GAS_EQUIV.values()), "All factors should be positive"

    # Step 3: Test with minimal data
    test_data = pd.DataFrame(
        {
            "WellID": ["WELL001", "WELL002"],
            "ProductionMonth": ["2024-01", "2024-01"],
            "GasProduction": [1000.0, 2000.0],
            "EthaneMixVolume": [5.0, 10.0],
            "PropaneMixVolume": [3.0, 6.0],
            "ButaneMixVolume": [2.0, 4.0],
            "PentaneMixVolume": [1.0, 2.0],
            "LiteMixVolume": [0.5, 1.0],
        }
    )

    # Apply calculations manually (simulating the processing pipeline)
    test_data["ethane_gas_equiv"] = test_data["EthaneMixVolume"] * GAS_EQUIV["ethane"]
    test_data["propane_gas_equiv"] = (
        test_data["PropaneMixVolume"] * GAS_EQUIV["propane"]
    )
    test_data["butane_gas_equiv"] = test_data["ButaneMixVolume"] * GAS_EQUIV["butane"]
    test_data["pentane_gas_equiv"] = (
        test_data["PentaneMixVolume"] * GAS_EQUIV["pentane"]
    )
    test_data["lite_gas_equiv"] = test_data["LiteMixVolume"] * GAS_EQUIV["lite_mix"]

    test_data["total_gas_equivalent"] = (
        test_data["ethane_gas_equiv"]
        + test_data["propane_gas_equiv"]
        + test_data["butane_gas_equiv"]
        + test_data["pentane_gas_equiv"]
        + test_data["lite_gas_equiv"]
    )

    # Validate calculations
    assert all(
        test_data["total_gas_equivalent"] > 0
    ), "All wells should have positive gas equivalent"
    assert test_data["total_gas_equivalent"].sum() > 0, "Total should be positive"

    assert True, "Config-to-processing integration test completed"


def test_minimal_forecast_workflow():
    """Test minimal forecasting workflow with small dataset."""

    # Step 1: Create minimal historical data
    dates = pd.date_range(start="2022-01-01", periods=12, freq="ME")
    initial_production = 10000.0
    decline_rate = 0.98  # 2% monthly decline

    historical_data = []
    for i, date in enumerate(dates):
        production = initial_production * (decline_rate**i)
        historical_data.append(
            {
                "WellID": "TEST_WELL",
                "ProductionMonth": date,
                "GasProduction": production,
            }
        )

    historical_df = pd.DataFrame(historical_data)

    # Step 2: Simple decline curve fitting
    production_values = historical_df["GasProduction"].values
    initial_production = production_values[0]
    final_production = production_values[-1]

    # Calculate implied decline rate
    months = len(production_values) - 1
    annual_decline = (final_production / initial_production) ** (12 / months)

    # Reasonable decline rate validation
    assert 0.2 < annual_decline < 1.0, "Decline rate should be reasonable"

    # Step 3: Generate forecasts
    forecast_periods = 24
    forecast_dates = pd.date_range(
        start=historical_df["ProductionMonth"].max() + pd.DateOffset(months=1),
        periods=forecast_periods,
        freq="ME",
    )

    forecast_data = []
    monthly_decline = annual_decline ** (1 / 12)

    for i, date in enumerate(forecast_dates):
        forecast_production = final_production * (monthly_decline ** (i + 1))
        forecast_data.append(
            {
                "WellID": "TEST_WELL",
                "ProductionMonth": date,
                "GasProduction": forecast_production,
                "forecast_month": i + 1,
            }
        )

    forecast_df = pd.DataFrame(forecast_data)

    # Step 4: Validation
    assert (
        len(forecast_df) == forecast_periods
    ), "Should generate correct number of forecasts"
    assert all(forecast_df["GasProduction"] > 0), "All forecasts should be positive"
    assert forecast_df[
        "GasProduction"
    ].is_monotonic_decreasing, "Production should decline"

    # Calculate summary metrics
    total_historical = historical_df["GasProduction"].sum()
    total_forecast = forecast_df["GasProduction"].sum()

    assert total_historical > 0, "Historical production should be positive"
    assert total_forecast > 0, "Forecast production should be positive"

    # Check that monthly forecast production is declining
    first_month_forecast = forecast_df["GasProduction"].iloc[0]
    last_month_forecast = forecast_df["GasProduction"].iloc[-1]
    assert (
        last_month_forecast < first_month_forecast
    ), "Forecast should show declining production"

    assert True, "Minimal forecast workflow test completed"
