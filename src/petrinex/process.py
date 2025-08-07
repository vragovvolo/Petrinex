import logging
from typing import Dict, Optional, List, Tuple
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, to_timestamp, regexp_replace, trim
from pyspark.sql.types import StringType, DoubleType, TimestampType

from petrinex.schemas import get_schema

logger = logging.getLogger(__name__)

def process_csv_to_bronze(spark, config, dataset: str) -> tuple:
    """Simple CSV to Delta tables processing for local development."""

    assert dataset in ["conventional", "ngl"], "Invalid dataset"
    data_config = getattr(config, dataset)

    csv_path = f"{config.download_dir}/{dataset}/*.CSV"
    bronze_table = f"{config.catalog}.{config.schema}.{dataset}_bronze"

    schema = get_schema(data_config.code.lower(), "bronze")

    raw_df = spark.read.option("header", True).schema(schema).csv(csv_path)

    bronze_df = _clean_data(raw_df, data_config.code.lower())
    bronze_df.write.mode("overwrite").saveAsTable(bronze_table)

    return bronze_df


def process_bronze_to_silver(spark, config, dataset: str) -> tuple:
    """Process bronze data to silver."""
    data_config = getattr(config, dataset)
    bronze_table = f"{config.catalog}.{config.schema}.{dataset}_bronze"
    silver_table = f"{config.catalog}.{config.schema}.{dataset}_silver"

    bronze_df = spark.table(bronze_table)

    silver_df = bronze_df.withColumn(
        "ProductionMonth", to_timestamp(col("ProductionMonth"), "yyyy-MM")
    )
    silver_df.write.mode("overwrite").saveAsTable(silver_table)

    return silver_df


def _clean_data(df: DataFrame, table_code: str) -> DataFrame:
    """Clean data - convert strings to numbers, handle nulls."""
    if table_code == "vol":
        return _clean_vol(df)
    elif table_code == "ngl":
        return _clean_ngl(df)
    return df


def _clean_vol(df: DataFrame) -> DataFrame:
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


def _clean_ngl(df: DataFrame) -> DataFrame:
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

    # Ensure production values are non-negative
    production_cols = ["GasProduction", "OilProduction", "CondensateProduction"]
    for col_name in production_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, when(col(col_name) < 0, lit(0.0)).otherwise(col(col_name))
            )

    # Clean key string columns
    for col_name in ["OperatorBAID", "ReportingFacilityID", "WellID"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))

    return df
