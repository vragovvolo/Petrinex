import logging
from pyspark.sql import DataFrame, SparkSession
from petrinex.config import PetrinexConfig, DatasetConfig

logger = logging.getLogger(__name__)


def process_csvs_to_delta(
    config: PetrinexConfig,
    dataset: DatasetConfig,
    csv_directory: str,
    spark: SparkSession,
) -> DataFrame:
    """
    Process CSV files from download directory and write to Delta table.

    Args:
        config: Main configuration object
        dataset: Dataset-specific configuration
        csv_directory: Directory containing CSV files
        spark: Spark session

    Returns:
        DataFrame that was written to Delta
    """
    # Use the CSV directory path directly (works for both Volumes and local paths)
    csv_path_pattern = f"{csv_directory}/*.CSV"

    logger.info(f"Reading CSV files from pattern: {csv_path_pattern}")

    # Read CSV files into DataFrame
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("pathGlobFilter", "*.CSV")
        .csv(csv_path_pattern)
    )

    # Create catalog and schema if they don't exist
    logger.info(
        f"Ensuring catalog {config.catalog} and schema {config.schema_name} exist"
    )
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema_name}")

    # Get full table name
    full_table_name = f"{config.catalog}.{config.schema_name}.{dataset.table_name}"

    # Write DataFrame to Delta table
    logger.info(f"Writing DataFrame to Delta table: {full_table_name}")
    df.write.option("overwriteSchema", "true").mode("overwrite").format(
        "delta"
    ).saveAsTable(full_table_name)

    row_count = df.count()
    logger.info(
        f"{dataset.name} data successfully written to {full_table_name} ({row_count:,} rows)"
    )

    return df
