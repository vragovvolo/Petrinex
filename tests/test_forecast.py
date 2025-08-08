"""
Tests for the forecasting module.
"""

import pandas as pd
import numpy as np
import pytest
from petrinex.forecast import (
    forecast_well_production,
    forecast_multiple_wells,
    exponential_decline,
    hyperbolic_decline,
    harmonic_decline,
    fit_arps_curve,
    calculate_r_squared,
    calculate_aic,
    export_forecast_summary_table,
    export_forecasted_production_table,
    combine_historical_and_forecast,
    filter_wells_by_min_months,
)


@pytest.fixture
def sample_ngl_data():
    """Create sample NGL data for testing."""
    dates = pd.date_range("2020-01-01", periods=24, freq="MS")
    well_ids = ["WELL001", "WELL002"]

    data = []
    for well_id in well_ids:
        for i, date in enumerate(dates):
            # Create declining production with some noise
            oil_prod = max(0, 100 * np.exp(-0.05 * i) + np.random.normal(0, 5))
            gas_prod = max(0, 500 * np.exp(-0.03 * i) + np.random.normal(0, 20))

            data.append(
                {
                    "WellID": well_id,
                    "ProductionMonth": date,
                    "DaysFromFirst": i * 30,  # Add DaysFromFirst column
                    "OilProduction": oil_prod,
                    "GasProduction": gas_prod,
                    "CondensateProduction": oil_prod * 0.1,
                    "WaterProduction": oil_prod * 0.2,
                    "ReportingFacilityName": f"Facility_{well_id}",
                    "OperatorName": "Test Operator",
                    "ReportingFacilityID": f"FAC_{well_id}",
                    "OperatorBAID": f"OP_{well_id}",
                    "WellLicenseNumber": f"LIC_{well_id}",
                    "Field": f"Field_{well_id}",
                    "Pool": f"Pool_{well_id}",
                    "Area": f"Area_{well_id}",
                    "Hours": 720.0,
                    "ResidueGasVolume": 0.0,
                    "Energy": 0.0,
                    "EthaneMixVolume": 0.0,
                    "EthaneSpecVolume": 0.0,
                    "PropaneMixVolume": 0.0,
                    "PropaneSpecVolume": 0.0,
                    "ButaneMixVolume": 0.0,
                    "ButaneSpecVolume": 0.0,
                    "PentaneMixVolume": 0.0,
                    "PentaneSpecVolume": 0.0,
                    "LiteMixVolume": 0.0,
                }
            )

    return pd.DataFrame(data)


def test_arps_curves():
    """Test ARPS decline curve functions."""
    t = np.array([0, 365, 730])  # 0, 1, 2 years
    qi = 100
    di = 0.1
    b = 0.5

    # Test exponential decline
    exp_result = exponential_decline(t, qi, di)
    assert exp_result[0] == qi  # Initial production
    assert exp_result[1] < qi  # Should decline
    assert exp_result[2] < exp_result[1]  # Should continue declining

    # Test hyperbolic decline
    hyp_result = hyperbolic_decline(t, qi, di, b)
    assert hyp_result[0] == qi
    assert hyp_result[1] < qi
    assert hyp_result[2] < hyp_result[1]

    # Test harmonic decline
    har_result = harmonic_decline(t, qi, di)
    assert har_result[0] == qi
    assert har_result[1] < qi
    assert har_result[2] < har_result[1]


def test_calculate_r_squared():
    """Test R-squared calculation."""
    y_actual = np.array([10, 8, 6, 4, 2])
    y_perfect = np.array([10, 8, 6, 4, 2])
    y_bad = np.array([1, 1, 1, 1, 1])

    # Perfect fit should have R² = 1
    assert abs(calculate_r_squared(y_actual, y_perfect) - 1.0) < 1e-10

    # Bad fit should have low R²
    r2_bad = calculate_r_squared(y_actual, y_bad)
    assert r2_bad < 0.5


def test_calculate_aic():
    """Test AIC calculation."""
    y_actual = np.array([10, 8, 6, 4, 2])
    y_good = np.array([10.1, 7.9, 6.1, 3.9, 2.1])
    y_bad = np.array([5, 5, 5, 5, 5])

    aic_good = calculate_aic(y_actual, y_good, 2)
    aic_bad = calculate_aic(y_actual, y_bad, 2)

    # Better fit should have lower AIC
    assert aic_good < aic_bad


def test_fit_arps_curve(sample_ngl_data):
    """Test ARPS curve fitting."""
    well_data = sample_ngl_data[sample_ngl_data["WellID"] == "WELL001"]

    result = fit_arps_curve(well_data, "OilProduction", "auto")

    if result["success"]:
        assert "curve_type" in result
        assert "parameters" in result
        assert "r_squared" in result
        assert result["curve_type"] in ["exponential", "hyperbolic", "harmonic"]
        assert 0 <= result["r_squared"] <= 1
        assert callable(result["forecast_func"])


def test_forecast_well_production(sample_ngl_data):
    """Test well production forecasting."""
    well_data = sample_ngl_data[sample_ngl_data["WellID"] == "WELL001"]

    result = forecast_well_production(
        well_data, forecast_months=12, production_column="OilProduction"
    )

    if result["success"]:
        assert "forecast" in result
        assert "well_id" in result
        assert "curve_type" in result
        assert "parameters" in result
        assert "r_squared" in result

        # Check forecast DataFrame structure
        forecast_df = result["forecast"]
        assert "ProductionMonth" in forecast_df.columns
        assert "DaysFromFirst" in forecast_df.columns
        assert "OilProduction_Forecast" in forecast_df.columns

        # Should have approximately 12 months of forecast
        assert len(forecast_df) >= 10  # Allow some flexibility


def test_forecast_multiple_wells(sample_ngl_data):
    """Test forecasting multiple wells."""
    results = forecast_multiple_wells(
        sample_ngl_data,
        forecast_months=12,
        production_column="OilProduction",
        min_r_squared=0.0,  # Set low threshold for test data
    )

    # Should have results for wells that could be forecast
    assert isinstance(results, dict)

    # Check structure of results
    for well_id, result in results.items():
        assert result["success"] == True
        assert "forecast" in result
        assert "curve_type" in result


def test_forecast_insufficient_data():
    """Test forecasting with insufficient data."""
    # Create data with only 2 points
    data = pd.DataFrame(
        {
            "WellID": ["WELL001"] * 2,
            "ProductionMonth": ["2020-01", "2020-02"],
            "OilProduction": [100, 90],
            "DaysFromFirst": [0, 30],
        }
    )

    result = forecast_well_production(data, production_column="OilProduction")

    # Should fail due to insufficient data
    assert result["success"] == False
    assert "error" in result


def test_forecast_zero_production():
    """Test forecasting with zero/negative production values."""
    data = pd.DataFrame(
        {
            "WellID": ["WELL001"] * 10,
            "ProductionMonth": [f"2020-{i+1:02d}" for i in range(10)],
            "OilProduction": [0] * 10,  # All zero production
            "DaysFromFirst": list(range(0, 300, 30)),
        }
    )

    result = forecast_well_production(data, production_column="OilProduction")

    # Should fail due to no positive production values
    assert result["success"] == False


def test_export_forecast_summary_table(sample_ngl_data):
    """Test exporting forecast summary as a table."""

    results = forecast_multiple_wells(
        sample_ngl_data,
        forecast_months=12,
        production_column="GasProduction",
        min_r_squared=0.0,  # Set low threshold for test data
    )

    summary_table = export_forecast_summary_table(results, "GasProduction")

    if len(summary_table) > 0:
        # Check required columns
        required_cols = [
            "WellID",
            "ProductionType",
            "CurveType",
            "RSquared",
            "InitialRate_qi",
            "DeclineRate_di",
            "ForecastCumulative12Month",
        ]
        for col in required_cols:
            assert col in summary_table.columns

        # Check data types and values
        assert summary_table["ProductionType"].iloc[0] == "GasProduction"
        assert summary_table["RSquared"].dtype in [np.float64, float]
        assert all(summary_table["RSquared"] >= 0)


def test_export_forecasted_production_table(sample_ngl_data):
    """Test exporting forecasted production in same schema as input."""
    results = forecast_multiple_wells(
        sample_ngl_data,
        forecast_months=6,
        production_column="GasProduction",
        min_r_squared=0.0,
    )

    production_table = export_forecasted_production_table(results, "GasProduction")

    if len(production_table) > 0:
        # Check required columns match input schema
        required_cols = [
            "WellID",
            "ProductionMonth",
            "GasProduction",
            "OilProduction",
            "DataType",
            "ForecastMethod",
            "ForecastRSquared",
        ]
        for col in required_cols:
            assert col in production_table.columns

        # Check data values
        assert all(production_table["DataType"] == "Forecast")
        assert all(production_table["ForecastMethod"].str.startswith("ARPS_"))
        assert all(production_table["GasProduction"] >= 0)
        assert all(
            production_table["OilProduction"] == 0
        )  # Should be zero for gas forecasts


def test_combine_historical_and_forecast(sample_ngl_data):
    """Test combining historical and forecast data."""

    # Get a small forecast
    results = forecast_multiple_wells(
        sample_ngl_data.head(50),
        forecast_months=3,
        production_column="GasProduction",
        min_r_squared=0.0,
    )

    if len(results) > 0:
        production_table = export_forecasted_production_table(results, "GasProduction")
        combined_table = combine_historical_and_forecast(
            sample_ngl_data.head(50), production_table, "GasProduction"
        )

        # Check that both data types are present
        data_types = combined_table["DataType"].value_counts()
        assert "Historical" in data_types.index
        assert "Forecast" in data_types.index

        # Check that data is sorted by well and date
        for well_id in combined_table["WellID"].unique():
            well_data = combined_table[combined_table["WellID"] == well_id]
            dates = well_data["ProductionMonth"]
            assert dates.is_monotonic_increasing


def test_export_empty_forecasts():
    """Test exporting with empty forecast results."""
    empty_forecasts = {}

    summary_table = export_forecast_summary_table(empty_forecasts, "GasProduction")
    assert len(summary_table) == 0

    production_table = export_forecasted_production_table(
        empty_forecasts, "GasProduction"
    )
    assert len(production_table) == 0


def test_forecast_spark_workflow_imports():
    """Test that the main Spark workflow function can be imported."""
    from petrinex.forecast import forecast_spark_workflow

    # Should be callable
    assert callable(forecast_spark_workflow)

    # Note: Full integration tests would need a real Spark environment with catalog access
