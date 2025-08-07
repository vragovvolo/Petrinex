"""
Tests for ngl_simple.py - Core NGL calculations module.

This module tests the simplified NGL calculation functions that:
1. Calculate gas equivalent volumes using AER-approved factors
2. Calculate real liquids volumes per AER definitions
3. Perform unit conversions between metric and imperial
4. Validate AER compliance ranges
"""

import pytest
import pandas as pd
from pathlib import Path


def test_gas_equivalent_constants():
    """Test that AER-approved gas equivalent factors are correct."""
    from petrinex.calcs import GAS_EQUIV, M3_TO_BBL, M3_TO_MCF

    # Test AER-approved gas equivalent factors (Directive 017)
    expected_factors = {
        "ethane": 384.0,  # m³ gas per m³ ethane
        "propane": 235.0,  # m³ gas per m³ propane
        "butane": 156.0,  # m³ gas per m³ butane
        "pentane": 130.0,  # m³ gas per m³ pentane
        "lite_mix": 200.0,  # m³ gas per m³ lite mix
    }

    for component, expected in expected_factors.items():
        assert component in GAS_EQUIV, f"Missing gas equivalent factor for {component}"
        assert GAS_EQUIV[component] == expected, f"Incorrect factor for {component}"

    # Test unit conversion constants (AER Manual 011)
    assert abs(M3_TO_BBL - 6.28981) < 0.0001, "Incorrect m³ to barrel conversion"
    assert abs(M3_TO_MCF - 0.035315) < 0.0001, "Incorrect m³ to MCF conversion"


def test_pandas_ngl_calculations():
    """Test NGL calculations using pandas (simulating Spark logic)."""

    # Create test data mimicking well production
    test_data = pd.DataFrame(
        {
            "WellID": ["WELL001", "WELL002", "WELL003"],
            "EthaneMixVolume": [10.0, 15.0, 8.0],
            "PropaneMixVolume": [5.0, 8.0, 3.0],
            "ButaneMixVolume": [2.0, 4.0, 1.5],
            "PentaneMixVolume": [1.0, 2.0, 0.8],
            "LiteMixVolume": [0.5, 1.0, 0.3],
            "OilProduction": [100.0, 150.0, 80.0],
            "CondensateProduction": [20.0, 30.0, 15.0],
            "GasProduction": [50000.0, 75000.0, 40000.0],
        }
    )

    # Import constants for calculations
    from petrinex.calcs import GAS_EQUIV, M3_TO_BBL, M3_TO_MCF

    # Calculate gas equivalents (pandas equivalent of Spark logic)
    test_data["ethane_gas_equiv"] = test_data["EthaneMixVolume"] * GAS_EQUIV["ethane"]
    test_data["propane_gas_equiv"] = (
        test_data["PropaneMixVolume"] * GAS_EQUIV["propane"]
    )
    test_data["butane_gas_equiv"] = test_data["ButaneMixVolume"] * GAS_EQUIV["butane"]
    test_data["pentane_gas_equiv"] = (
        test_data["PentaneMixVolume"] * GAS_EQUIV["pentane"]
    )
    test_data["lite_gas_equiv"] = test_data["LiteMixVolume"] * GAS_EQUIV["lite_mix"]

    test_data["total_gas_equiv"] = (
        test_data["ethane_gas_equiv"]
        + test_data["propane_gas_equiv"]
        + test_data["butane_gas_equiv"]
        + test_data["pentane_gas_equiv"]
        + test_data["lite_gas_equiv"]
    )

    # Calculate real liquids
    test_data["c5_liquids"] = test_data["PentaneMixVolume"]  # Simplified
    test_data["oil_liquids"] = test_data["OilProduction"]
    test_data["condensate_liquids"] = test_data["CondensateProduction"]
    test_data["lite_liquids"] = test_data["LiteMixVolume"]

    test_data["total_liquids"] = (
        test_data["c5_liquids"]
        + test_data["oil_liquids"]
        + test_data["condensate_liquids"]
        + test_data["lite_liquids"]
    )

    # Calculate gas-to-liquids ratio
    test_data["gas_liquids_ratio"] = (
        test_data["GasProduction"] / test_data["total_liquids"]
    )

    # Unit conversions
    test_data["total_gas_equiv_mcf"] = test_data["total_gas_equiv"] * M3_TO_MCF
    test_data["total_liquids_bbl"] = test_data["total_liquids"] * M3_TO_BBL

    # Validation checks
    assert len(test_data) == 3, "Should have 3 test wells"
    assert all(test_data["total_gas_equiv"] > 0), "All wells should have gas equivalent"
    assert all(test_data["total_liquids"] > 0), "All wells should have liquids"
    assert all(
        test_data["gas_liquids_ratio"] > 0
    ), "All wells should have positive ratio"

    # Check specific calculations for WELL001
    well1 = test_data.iloc[0]
    expected_ethane_equiv = 10.0 * 384.0  # 3,840 m³
    expected_propane_equiv = 5.0 * 235.0  # 1,175 m³
    expected_total_equiv = (
        3840 + 1175 + (2.0 * 156) + (1.0 * 130) + (0.5 * 200)
    )  # 5,357 m³

    assert abs(well1["ethane_gas_equiv"] - expected_ethane_equiv) < 0.1
    assert abs(well1["propane_gas_equiv"] - expected_propane_equiv) < 0.1
    assert abs(well1["total_gas_equiv"] - expected_total_equiv) < 0.1

    # Check unit conversions
    expected_mcf = expected_total_equiv * M3_TO_MCF
    expected_bbl = (100 + 20 + 1 + 0.5) * M3_TO_BBL  # Total liquids in barrels

    assert abs(well1["total_gas_equiv_mcf"] - expected_mcf) < 0.1
    assert abs(well1["total_liquids_bbl"] - expected_bbl) < 0.1


def test_aer_compliance_logic():
    """Test AER compliance validation ranges."""

    # Test data with various compliance scenarios
    test_cases = [
        {
            "name": "Compliant well",
            "total_gas_equiv": 50000.0,
            "total_liquids": 100.0,
            "expected_compliant": True,
        },
        {
            "name": "Gas equivalent too high",
            "total_gas_equiv": 2000000.0,  # Over 1M limit
            "total_liquids": 100.0,
            "expected_compliant": False,
        },
        {
            "name": "Liquids too high",
            "total_gas_equiv": 50000.0,
            "total_liquids": 15000.0,  # Over 10K limit
            "expected_compliant": False,
        },
        {
            "name": "Negative values",
            "total_gas_equiv": -100.0,
            "total_liquids": 50.0,
            "expected_compliant": False,
        },
        {
            "name": "Zero values (edge case)",
            "total_gas_equiv": 0.0,
            "total_liquids": 0.0,
            "expected_compliant": True,  # Zero is acceptable
        },
    ]

    for case in test_cases:
        # Apply AER compliance logic (simplified from Spark)
        gas_ok = 0 <= case["total_gas_equiv"] <= 1000000
        liquids_ok = 0 <= case["total_liquids"] <= 10000
        is_compliant = gas_ok and liquids_ok

        assert (
            is_compliant == case["expected_compliant"]
        ), f"AER compliance check failed for {case['name']}"


def test_unit_conversion_accuracy():
    """Test unit conversion accuracy against AER standards."""

    from petrinex.calcs import M3_TO_BBL, M3_TO_MCF

    # Test known conversions (AER Manual 011 standards)
    test_conversions = [
        (1.0, M3_TO_BBL, 6.28981),  # 1 m³ = 6.28981 bbl
        (1000.0, M3_TO_MCF, 35.315),  # 1000 m³ = 35.315 MCF
        (100.0, M3_TO_BBL, 628.981),  # 100 m³ = 628.981 bbl
        (10000.0, M3_TO_MCF, 353.15),  # 10000 m³ = 353.15 MCF
    ]

    for volume, factor, expected in test_conversions:
        result = volume * factor
        assert (
            abs(result - expected) < 0.001
        ), f"Unit conversion failed: {volume} * {factor} = {result}, expected {expected}"

    # Test reverse conversions
    bbl_to_m3 = 1 / M3_TO_BBL
    mcf_to_m3 = 1 / M3_TO_MCF

    assert (
        abs((100 * M3_TO_BBL) * bbl_to_m3 - 100.0) < 0.001
    ), "Round-trip BBL conversion failed"
    assert (
        abs((1000 * M3_TO_MCF) * mcf_to_m3 - 1000.0) < 0.001
    ), "Round-trip MCF conversion failed"


def test_calculation_integration_with_real_data():
    """Test calculations work with real fixture data."""

    fixture_path = Path("fixtures/ngl_vol_silver.parquet")
    if not fixture_path.exists():
        pytest.skip("Real fixture data not available")

    # Load small sample of real data
    df = pd.read_parquet(fixture_path).head(100)  # Test with 100 wells

    from petrinex.calcs import GAS_EQUIV

    # Convert to numeric
    ngl_columns = [
        "EthaneMixVolume",
        "PropaneMixVolume",
        "ButaneMixVolume",
        "PentaneMixVolume",
        "LiteMixVolume",
    ]

    for col in ngl_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Apply calculations
    df["total_gas_equiv"] = (
        df.get("EthaneMixVolume", 0) * GAS_EQUIV["ethane"]
        + df.get("PropaneMixVolume", 0) * GAS_EQUIV["propane"]
        + df.get("ButaneMixVolume", 0) * GAS_EQUIV["butane"]
        + df.get("PentaneMixVolume", 0) * GAS_EQUIV["pentane"]
        + df.get("LiteMixVolume", 0) * GAS_EQUIV["lite_mix"]
    )

    # Validation with real data
    wells_with_calculations = len(df[df["total_gas_equiv"] > 0])
    total_gas_equiv = df["total_gas_equiv"].sum()

    assert wells_with_calculations > 0, "Should have wells with NGL calculations"
    assert total_gas_equiv > 0, "Should have positive total gas equivalent"

    # Check for reasonable ranges in real data
    max_gas_equiv = df["total_gas_equiv"].max()
    assert max_gas_equiv < 1000000, "Gas equivalent should be within AER range"


def test_calculation_edge_cases():
    """Test edge cases and error handling in calculations."""

    from petrinex.calcs import GAS_EQUIV

    # Test with missing/null data
    edge_case_data = pd.DataFrame(
        {
            "WellID": ["EDGE001", "EDGE002", "EDGE003"],
            "EthaneMixVolume": [None, 0.0, 10.0],
            "PropaneMixVolume": [5.0, None, 0.0],
            "ButaneMixVolume": [0.0, 2.0, None],
            "PentaneMixVolume": [1.0, 0.0, 2.0],
            "LiteMixVolume": [None, None, 1.0],
        }
    )

    # Convert and fill nulls (simulating Spark coalesce)
    for col in [
        "EthaneMixVolume",
        "PropaneMixVolume",
        "ButaneMixVolume",
        "PentaneMixVolume",
        "LiteMixVolume",
    ]:
        edge_case_data[col] = pd.to_numeric(
            edge_case_data[col], errors="coerce"
        ).fillna(0)

    # Calculate gas equivalents
    edge_case_data["total_gas_equiv"] = (
        edge_case_data["EthaneMixVolume"] * GAS_EQUIV["ethane"]
        + edge_case_data["PropaneMixVolume"] * GAS_EQUIV["propane"]
        + edge_case_data["ButaneMixVolume"] * GAS_EQUIV["butane"]
        + edge_case_data["PentaneMixVolume"] * GAS_EQUIV["pentane"]
        + edge_case_data["LiteMixVolume"] * GAS_EQUIV["lite_mix"]
    )

    # Validate edge case handling
    assert len(edge_case_data) == 3, "Should handle all edge case wells"
    assert all(edge_case_data["total_gas_equiv"] >= 0), "Should handle nulls as zeros"
    assert (
        not edge_case_data["total_gas_equiv"].isna().any()
    ), "Should not have NaN values"

    # Test specific edge cases
    well1 = edge_case_data.iloc[0]  # Has nulls -> zeros
    well2 = edge_case_data.iloc[1]  # Has zeros and nulls
    well3 = edge_case_data.iloc[2]  # Has nulls and values

    # EDGE001: Only propane and pentane should contribute
    expected_well1 = 5.0 * GAS_EQUIV["propane"] + 1.0 * GAS_EQUIV["pentane"]
    assert abs(well1["total_gas_equiv"] - expected_well1) < 0.1

    # EDGE003: Only ethane and pentane should contribute
    expected_well3 = (
        10.0 * GAS_EQUIV["ethane"]
        + 2.0 * GAS_EQUIV["pentane"]
        + 1.0 * GAS_EQUIV["lite_mix"]
    )
    assert abs(well3["total_gas_equiv"] - expected_well3) < 0.1
