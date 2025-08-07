"""Simple NGL calculations with AER compliance and unit conversions."""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, coalesce

logger = logging.getLogger(__name__)

# AER-approved factors
GAS_EQUIV = {
    "ethane": 384.0,
    "propane": 235.0,
    "butane": 156.0,
    "pentane": 130.0,
    "lite_mix": 200.0,
}
M3_TO_BBL = 6.28981
M3_TO_MCF = 0.035315


def add_ngl_calculations(df: DataFrame) -> DataFrame:
    """Add all NGL calculations in one function."""

    # Gas equivalent volumes
    df = df.withColumn(
        "ethane_gas_equiv",
        coalesce(col("EthaneMixVolume"), lit(0.0)) * lit(GAS_EQUIV["ethane"]),
    )
    df = df.withColumn(
        "propane_gas_equiv",
        coalesce(col("PropaneMixVolume"), lit(0.0)) * lit(GAS_EQUIV["propane"]),
    )
    df = df.withColumn(
        "butane_gas_equiv",
        coalesce(col("ButaneMixVolume"), lit(0.0)) * lit(GAS_EQUIV["butane"]),
    )
    df = df.withColumn(
        "pentane_gas_equiv",
        coalesce(col("PentaneMixVolume"), lit(0.0)) * lit(GAS_EQUIV["pentane"]),
    )
    df = df.withColumn(
        "lite_gas_equiv",
        coalesce(col("LiteMixVolume"), lit(0.0)) * lit(GAS_EQUIV["lite_mix"]),
    )

    df = df.withColumn(
        "total_gas_equiv",
        col("ethane_gas_equiv")
        + col("propane_gas_equiv")
        + col("butane_gas_equiv")
        + col("pentane_gas_equiv")
        + col("lite_gas_equiv"),
    )

    # Real liquids (C5+, oil, condensate)
    df = df.withColumn(
        "c5_liquids",
        coalesce(col("PentaneMixVolume"), lit(0.0))
        + coalesce(col("PentaneSpecVolume"), lit(0.0)),
    )
    df = df.withColumn("oil_liquids", coalesce(col("OilProduction"), lit(0.0)))
    df = df.withColumn(
        "condensate_liquids", coalesce(col("CondensateProduction"), lit(0.0))
    )
    df = df.withColumn("lite_liquids", coalesce(col("LiteMixVolume"), lit(0.0)))

    df = df.withColumn(
        "total_liquids",
        col("c5_liquids")
        + col("oil_liquids")
        + col("condensate_liquids")
        + col("lite_liquids"),
    )

    # Gas-to-liquids ratio
    df = df.withColumn(
        "gas_liquids_ratio",
        when(
            col("total_liquids") > 0,
            coalesce(col("GasProduction"), lit(0.0)) / col("total_liquids"),
        ).otherwise(lit(None)),
    )

    # Simple AER compliance check (values within reasonable ranges)
    df = df.withColumn(
        "aer_compliant",
        (col("total_gas_equiv") >= 0)
        & (col("total_gas_equiv") <= 1000000)
        & (col("total_liquids") >= 0)
        & (col("total_liquids") <= 10000),
    )

    return df


def calculate_ngl_summary(df: DataFrame) -> dict:
    """Get simple summary stats."""
    stats = df.agg(
        {"total_gas_equiv": "sum", "total_liquids": "sum", "GasProduction": "sum"}
    ).collect()[0]

    return {
        "total_wells": df.count(),
        "total_gas_equivalent": stats[0] or 0,
        "total_real_liquids": stats[1] or 0,
        "total_gas_production": stats[2] or 0,
    }
