import logging
from typing import Dict, Optional, List, Tuple
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    to_timestamp,
    regexp_replace,
    trim,
    monotonically_increasing_id,
    input_file_name,
)
from pyspark.sql.types import StringType, DoubleType, TimestampType

# Config import removed - using simple config objects
from petrinex.schemas import get_schema

logger = logging.getLogger(__name__)


def process_csv_to_tables(config, dataset: str, spark) -> tuple:
    """Simple CSV to Delta tables processing."""

    # Basic setup
    table_type = "conv_vol" if dataset == "conventional" else "ngl_vol"
    csv_path = f"/{config.ingest.download_dir}/{dataset}/*.CSV"
    bronze_table = f"{config.catalog}.{config.schema}.{dataset}_bronze"
    silver_table = f"{config.catalog}.{config.schema}.{dataset}_silver"

    logger.info(f"Processing {dataset}: {csv_path} -> {bronze_table}, {silver_table}")

    # Read CSV files
    schema = get_schema(table_type, "bronze")
    raw_df = spark.read.option("header", True).schema(schema).csv(csv_path)

    # Clean data for bronze
    bronze_df = _clean_data(raw_df, table_type)

    # Create silver with timestamps
    silver_df = bronze_df.withColumn(
        "ProductionMonth", to_timestamp(col("ProductionMonth"), "yyyy-MM")
    )

    # Write tables
    bronze_df.write.mode("overwrite").format("delta").saveAsTable(bronze_table)
    silver_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)

    logger.info(f"Wrote {bronze_df.count()} rows to {bronze_table} and {silver_table}")

    return bronze_df, silver_df


def _clean_data(df: DataFrame, table_type: str) -> DataFrame:
    """Clean data - convert strings to numbers, handle nulls."""
    if table_type == "conv_vol":
        return _clean_conv_vol(df)
    elif table_type == "ngl_vol":
        return _clean_ngl_vol(df)
    return df


def _clean_conv_vol(df: DataFrame) -> DataFrame:
    """Clean conventional data - just convert numbers and clean strings."""
    # Convert numeric columns
    for col_name in ["Volume", "Energy", "Hours"]:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                regexp_replace(col(col_name), r"[^\d.-]", "").cast(DoubleType()),
            )

    # Clean string columns
    for col_name in ["OperatorBAID", "ReportingFacilityID"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))

    return df


def _clean_ngl_vol(df: DataFrame) -> DataFrame:
    """Clean NGL data - convert production numbers and clean key strings."""
    # Convert numeric production columns
    production_cols = [
        "GasProduction",
        "OilProduction",
        "CondensateProduction",
        "Hours",
        "EthaneMixVolume",
        "PropaneMixVolume",
        "ButaneMixVolume",
        "PentaneMixVolume",
        "LiteMixVolume",
    ]

    for col_name in production_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                regexp_replace(col(col_name), r"[^\d.-]", "").cast(DoubleType()),
            )

    # Clean key string columns
    for col_name in ["OperatorBAID", "ReportingFacilityID", "WellID"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))

    return df


def prepare_data_for_forecasting(
    spark, config, table_type: str = "ngl_vol"
) -> DataFrame:
    """Load silver data and clean for forecasting."""
    table_name = f"{config.catalog}.{config.schema}.{table_type}_silver"
    df = spark.table(table_name)

    # Just ensure production values are non-negative
    production_cols = ["GasProduction", "OilProduction", "CondensateProduction"]
    for col_name in production_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, when(col(col_name) < 0, lit(0.0)).otherwise(col(col_name))
            )

    return df
