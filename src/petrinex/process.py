import logging
from typing import Dict, Optional, List, Tuple
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    when,
    lit,
    to_timestamp,
    regexp_replace,
    trim,
    min as spark_min,
    max as spark_max,
    datediff,
    month,
    sum,
    collect_list,
    struct,
)
from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.window import Window

from petrinex.schemas import get_schema
from petrinex.utils import table_exists
from petrinex.validation import validate_silver, validate_calendar

logger = logging.getLogger(__name__)


def process_csv_to_bronze(spark, config, dataset: str) -> tuple:
    """Process CSV to bronze tables using defined schemas."""

    assert dataset in ["conventional", "ngl"], "Invalid dataset"
    data_config = getattr(config, dataset)

    csv_path = f"{config.download_dir}/{dataset}/*.CSV"
    bronze_table = f"{config.catalog}.{config.schema}.{dataset}_bronze"

    # Get the bronze schema - this defines string types for initial reading
    bronze_schema = get_schema(data_config.code.lower(), "bronze")

    # Read CSV with header but without enforcing schema first (strings only)
    raw_df = spark.read.option("header", True).csv(csv_path)

    # Clean and transform data according to the dataset type
    bronze_df = _clean_data(raw_df, data_config.code.lower(), config, bronze_schema)
    bronze_df = _validate_schema_constraints(bronze_df, bronze_schema)

    bronze_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        bronze_table
    )

    return bronze_df


def compute_ngl_calendar(base_df: DataFrame) -> DataFrame:
    """
    Compute calendar view from base NGL silver DataFrame with cumulative production.

    Args:
        base_df: Spark DataFrame representing base NGL silver data

    Returns:
        DataFrame with monthly volumes and cumulative columns per well
    """
    # Ensure we have all required columns and handle nulls
    calendar_df = base_df.fillna(
        0.0,
        subset=[
            "Hours",
            "GasProduction",
            "OilProduction",
            "CondensateProduction",
            "WaterProduction",
        ],
    )

    # Calculate cumulative production for each well using window functions
    window_cumulative = (
        Window.partitionBy("WellID")
        .orderBy("ProductionMonth")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Add cumulative columns for main production types
    production_columns = [
        "GasProduction",
        "OilProduction",
        "CondensateProduction",
        "WaterProduction",
    ]

    for prod_col in production_columns:
        calendar_df = calendar_df.withColumn(
            f"{prod_col}Cumulative", sum(col(prod_col)).over(window_cumulative)
        )

    # Add cumulative hours
    calendar_df = calendar_df.withColumn(
        "HoursCumulative", sum(col("Hours")).over(window_cumulative)
    )

    return calendar_df


def create_ngl_calendar_silver(spark, config) -> DataFrame:
    """
    Create calendar silver table with monthly calendar volumes and cumulative production.

    This table contains:
    1. Monthly calendar day production volumes (as reported)
    2. Cumulative production from start of well
    3. All months including zero/downtime periods

    Args:
        spark: SparkSession
        config: Configuration object

    Returns:
        Calendar silver DataFrame
    """
    logger.info("Creating NGL calendar silver table with cumulative production")

    # Start with existing NGL silver table (which is the basic processed data)
    ngl_silver_table = f"{config.catalog}.{config.schema}.ngl_silver"
    base_df = spark.table(ngl_silver_table)

    # Compute calendar
    calendar_df = compute_ngl_calendar(base_df)

    # Validate calendar and backfill continuity
    try:
        validated_calendar_df, issues_df = validate_calendar(calendar_df)
        issues_table = f"{config.catalog}.{config.schema}.ngl_calendar_quality_issues"
        issues_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
            issues_table
        )
        logger.info(
            f"Wrote {issues_df.count():,} calendar quality issues to {issues_table}"
        )
        calendar_df = validated_calendar_df
    except Exception as e:
        logger.warning(f"Skipping calendar validation due to error: {e}")

    logger.info(f"Calendar silver table created with {calendar_df.count():,} records")

    return calendar_df


def compute_ngl_normalized(calendar_df: DataFrame) -> DataFrame:
    """
    Compute 730-hour normalized periods using cumulative differentials from calendar.

    This table:
    1. Uses cumulative production from calendar table
    2. Creates 730-hour normalized periods
    3. Calculates production for each period as differential of cumulative
    4. Estimates period start/end dates
    5. Computes daily rates for each period

    Args:
        spark: SparkSession
        config: Configuration object

    Returns:
        Normalized silver DataFrame
    """
    # Filter to wells with actual production and sufficient data
    producing_wells = (
        calendar_df.filter(
            (col("HoursCumulative") > 730)  # At least one full 730-hour period
            & (
                (col("GasProductionCumulative") > 0)
                | (col("OilProductionCumulative") > 0)
                | (col("CondensateProductionCumulative") > 0)
            )
        )
        .select("WellID")
        .distinct()
    )

    calendar_df = calendar_df.join(producing_wells, on="WellID", how="inner")

    # Create normalized periods using cumulative hours
    # Each period represents 730 hours of production
    normalized_records = []

    # Collect data by well for processing
    well_data = (
        calendar_df.groupBy("WellID")
        .agg(
            collect_list(
                struct(
                    col("ProductionMonth").alias("month"),
                    col("Hours").alias("hours"),
                    col("HoursCumulative").alias("hours_cum"),
                    col("GasProductionCumulative").alias("gas_cum"),
                    col("OilProductionCumulative").alias("oil_cum"),
                    col("CondensateProductionCumulative").alias("condensate_cum"),
                    col("WaterProductionCumulative").alias("water_cum"),
                    col("ReportingFacilityID").alias("facility_id"),
                    col("ReportingFacilityName").alias("facility_name"),
                    col("OperatorBAID").alias("operator_baid"),
                    col("OperatorName").alias("operator_name"),
                    col("WellLicenseNumber").alias("license"),
                    col("Field").alias("field"),
                    col("Pool").alias("pool"),
                    col("Area").alias("area"),
                )
            ).alias("records")
        )
        .collect()
    )

    # Process each well to create 730-hour periods
    from pyspark.sql.functions import lit

    normalized_data = []

    for row in well_data:
        well_id = row["WellID"]
        records = sorted(row["records"], key=lambda x: x["month"])

        if not records:
            continue

        # Get well metadata from first record
        first_record = records[0]
        well_metadata = {
            "ReportingFacilityID": first_record["facility_id"],
            "ReportingFacilityName": first_record["facility_name"],
            "OperatorBAID": first_record["operator_baid"],
            "OperatorName": first_record["operator_name"],
            "WellID": well_id,
            "WellLicenseNumber": first_record["license"],
            "Field": first_record["field"],
            "Pool": first_record["pool"],
            "Area": first_record["area"],
        }

        # Create 730-hour periods
        period_num = 1
        target_hours = 730

        while target_hours <= records[-1]["hours_cum"]:
            # Find the cumulative production at target_hours
            prev_cum_gas = 0
            prev_cum_oil = 0
            prev_cum_condensate = 0
            prev_cum_water = 0

            if period_num > 1:
                prev_target = (period_num - 1) * 730
                # Find production at previous period end
                for record in records:
                    if record["hours_cum"] >= prev_target:
                        # Interpolate if needed
                        if record["hours_cum"] == prev_target:
                            prev_cum_gas = record["gas_cum"]
                            prev_cum_oil = record["oil_cum"]
                            prev_cum_condensate = record["condensate_cum"]
                            prev_cum_water = record["water_cum"]
                        else:
                            # Linear interpolation between records
                            prev_record = None
                            for i, r in enumerate(records):
                                if r["hours_cum"] >= prev_target:
                                    prev_record = records[i - 1] if i > 0 else r
                                    break

                            if prev_record:
                                hours_diff = (
                                    record["hours_cum"] - prev_record["hours_cum"]
                                )
                                if hours_diff > 0:
                                    ratio = (
                                        prev_target - prev_record["hours_cum"]
                                    ) / hours_diff
                                    prev_cum_gas = prev_record["gas_cum"] + ratio * (
                                        record["gas_cum"] - prev_record["gas_cum"]
                                    )
                                    prev_cum_oil = prev_record["oil_cum"] + ratio * (
                                        record["oil_cum"] - prev_record["oil_cum"]
                                    )
                                    prev_cum_condensate = prev_record[
                                        "condensate_cum"
                                    ] + ratio * (
                                        record["condensate_cum"]
                                        - prev_record["condensate_cum"]
                                    )
                                    prev_cum_water = prev_record[
                                        "water_cum"
                                    ] + ratio * (
                                        record["water_cum"] - prev_record["water_cum"]
                                    )
                                else:
                                    prev_cum_gas = record["gas_cum"]
                                    prev_cum_oil = record["oil_cum"]
                                    prev_cum_condensate = record["condensate_cum"]
                                    prev_cum_water = record["water_cum"]
                        break

            # Find production at current period end
            curr_cum_gas = 0
            curr_cum_oil = 0
            curr_cum_condensate = 0
            curr_cum_water = 0
            period_end_date = None

            for record in records:
                if record["hours_cum"] >= target_hours:
                    if record["hours_cum"] == target_hours:
                        curr_cum_gas = record["gas_cum"]
                        curr_cum_oil = record["oil_cum"]
                        curr_cum_condensate = record["condensate_cum"]
                        curr_cum_water = record["water_cum"]
                        period_end_date = record["month"]
                    else:
                        # Linear interpolation
                        prev_record = None
                        for i, r in enumerate(records):
                            if r["hours_cum"] >= target_hours:
                                prev_record = records[i - 1] if i > 0 else r
                                break

                        if prev_record:
                            hours_diff = record["hours_cum"] - prev_record["hours_cum"]
                            if hours_diff > 0:
                                ratio = (
                                    target_hours - prev_record["hours_cum"]
                                ) / hours_diff
                                curr_cum_gas = prev_record["gas_cum"] + ratio * (
                                    record["gas_cum"] - prev_record["gas_cum"]
                                )
                                curr_cum_oil = prev_record["oil_cum"] + ratio * (
                                    record["oil_cum"] - prev_record["oil_cum"]
                                )
                                curr_cum_condensate = prev_record[
                                    "condensate_cum"
                                ] + ratio * (
                                    record["condensate_cum"]
                                    - prev_record["condensate_cum"]
                                )
                                curr_cum_water = prev_record["water_cum"] + ratio * (
                                    record["water_cum"] - prev_record["water_cum"]
                                )

                                # Estimate period end date
                                from datetime import datetime, timedelta

                                if isinstance(
                                    prev_record["month"], datetime
                                ) and isinstance(record["month"], datetime):
                                    month_diff = (
                                        record["month"] - prev_record["month"]
                                    ).days
                                    period_end_date = prev_record["month"] + timedelta(
                                        days=int(ratio * month_diff)
                                    )
                                else:
                                    period_end_date = record["month"]
                            else:
                                curr_cum_gas = record["gas_cum"]
                                curr_cum_oil = record["oil_cum"]
                                curr_cum_condensate = record["condensate_cum"]
                                curr_cum_water = record["water_cum"]
                                period_end_date = record["month"]
                    break

            # Calculate production for this period (differential)
            period_gas = curr_cum_gas - prev_cum_gas
            period_oil = curr_cum_oil - prev_cum_oil
            period_condensate = curr_cum_condensate - prev_cum_condensate
            period_water = curr_cum_water - prev_cum_water

            # Calculate daily rates (730 hours = 30.42 days)
            days_in_period = 730 / 24.0

            # Estimate period start date
            if period_num == 1:
                period_start_date = records[0]["month"]
            else:
                from datetime import timedelta

                if period_end_date:
                    period_start_date = period_end_date - timedelta(
                        days=int(days_in_period)
                    )
                else:
                    period_start_date = None

            # Create normalized record
            normalized_record = {
                **well_metadata,
                "NormalizedPeriod": period_num,
                "PeriodStartDate": period_start_date,
                "PeriodEndDate": period_end_date,
                "HoursInPeriod": 730.0,
                "GasProduction": float(period_gas),
                "OilProduction": float(period_oil),
                "CondensateProduction": float(period_condensate),
                "WaterProduction": float(period_water),
                "GasProductionDailyRate": (
                    float(period_gas / days_in_period) if days_in_period > 0 else 0.0
                ),
                "OilProductionDailyRate": (
                    float(period_oil / days_in_period) if days_in_period > 0 else 0.0
                ),
                "CondensateProductionDailyRate": (
                    float(period_condensate / days_in_period)
                    if days_in_period > 0
                    else 0.0
                ),
                "WaterProductionDailyRate": (
                    float(period_water / days_in_period) if days_in_period > 0 else 0.0
                ),
                "GasProductionCumulative": float(curr_cum_gas),
                "OilProductionCumulative": float(curr_cum_oil),
                "CondensateProductionCumulative": float(curr_cum_condensate),
                "WaterProductionCumulative": float(curr_cum_water),
                "HoursCumulative": float(target_hours),
                "DaysFromFirstProducing": int((target_hours - 730) / 24)
                + 30,  # Rough estimate
            }

            normalized_data.append(normalized_record)

            period_num += 1
            target_hours = period_num * 730

    # Convert to Spark DataFrame
    if normalized_data:
        normalized_df = calendar_df.sparkSession.createDataFrame(normalized_data)
        logger.info(
            f"Created {normalized_df.count():,} normalized periods for {len(well_data)} wells"
        )
    else:
        # Create empty DataFrame with schema
        from petrinex.schemas import NGL_NORMALIZED_SCHEMA

        normalized_df = calendar_df.sparkSession.createDataFrame(
            [], NGL_NORMALIZED_SCHEMA
        )
        logger.warning("No normalized periods created - insufficient data")

    return normalized_df


def create_ngl_normalized_silver(spark, config) -> DataFrame:
    """
    Create normalized silver table with 730-hour periods using cumulative differentials.

    Returns:
        Normalized silver DataFrame
    """
    logger.info("Creating NGL normalized silver table with 730-hour periods")

    # Start with calendar silver table (has cumulative production)
    calendar_table = f"{config.catalog}.{config.schema}.ngl_calendar"
    calendar_df = spark.table(calendar_table)

    return compute_ngl_normalized(calendar_df)


def create_ngl_calendar_silver_table(spark, config) -> str:
    """Create and save the calendar silver table."""
    calendar_df = create_ngl_calendar_silver(spark, config)
    calendar_table = f"{config.catalog}.{config.schema}.ngl_calendar"

    calendar_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        calendar_table
    )
    logger.info(f"Created calendar table: {calendar_table}")

    return calendar_table


def create_ngl_normalized_silver_table(spark, config) -> str:
    """Create and save the normalized silver table."""
    normalized_df = create_ngl_normalized_silver(spark, config)
    normalized_table = f"{config.catalog}.{config.schema}.ngl_normalized"

    normalized_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        normalized_table
    )
    logger.info(f"Created normalized table: {normalized_table}")

    return normalized_table


def build_ngl_calendar_and_normalized_tables(spark, config) -> Tuple[str, str]:
    """
    Ensure base NGL silver exists, then build and save `ngl_calendar` and `ngl_normalized` tables.

    Returns:
        Tuple of fully qualified table names: (calendar_table, normalized_table)
    """
    # Ensure base silver exists
    ngl_silver_table = f"{config.catalog}.{config.schema}.ngl_silver"
    if not table_exists(spark, ngl_silver_table):
        logger.info(
            "Base NGL silver table not found; creating it from bronze before building calendar/normalized"
        )
        process_bronze_to_silver(spark, config, dataset="ngl")

    # Build calendar and normalized tables
    calendar_table = create_ngl_calendar_silver_table(spark, config)
    normalized_table = create_ngl_normalized_silver_table(spark, config)

    return calendar_table, normalized_table


def process_bronze_to_silver(spark, config, dataset: str) -> tuple:
    """Process bronze data to silver using defined schemas."""
    data_config = getattr(config, dataset)
    bronze_table = f"{config.catalog}.{config.schema}.{dataset}_bronze"
    silver_table = f"{config.catalog}.{config.schema}.{dataset}_silver"

    # Get the silver schema
    silver_schema = get_schema(data_config.code.lower(), "silver")

    bronze_df = spark.table(bronze_table)

    # Transform ProductionMonth to timestamp with validation
    # First filter out rows with invalid/null ProductionMonth to enforce schema
    silver_df = bronze_df.filter(
        col("ProductionMonth").isNotNull() & (col("ProductionMonth") != "")
    ).withColumn(
        "ProductionMonth",
        when(
            col("ProductionMonth").rlike(r"^\d{4}-\d{2}$"),
            to_timestamp(col("ProductionMonth"), "yyyy-MM"),
        ).otherwise(to_timestamp(col("ProductionMonth"))),
    )

    # Ensure no null timestamps were created (enforce schema constraint)
    silver_df = silver_df.filter(col("ProductionMonth").isNotNull())

    # Add DaysFromFirst for NGL data (required for forecasting)
    if dataset == "ngl":
        silver_df = _add_days_from_first(silver_df)

    # Apply schema transformations and validate constraints
    silver_df = _apply_schema_transformations(silver_df, silver_schema)
    silver_df = _validate_schema_constraints(silver_df, silver_schema)

    # Domain validation and quality issues table
    try:
        if dataset == "ngl":
            production_cols = getattr(config.ngl, "production_columns", [])
        else:
            # For conventional, treat Volume as production (if present)
            production_cols = [c for c in ["Volume"] if c in silver_df.columns]

        validated_df, issues_df = validate_silver(silver_df, production_cols)
        issues_table = (
            f"{config.catalog}.{config.schema}.{dataset}_silver_quality_issues"
        )
        issues_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
            issues_table
        )
        logger.info(
            f"Wrote {issues_df.count():,} {dataset} silver quality issues to {issues_table}"
        )
        silver_df = validated_df
    except Exception as e:
        logger.warning(f"Skipping silver validation due to error: {e}")

    silver_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        silver_table
    )

    return silver_df


def _clean_data(df: DataFrame, table_code: str, config, target_schema) -> DataFrame:
    """Clean data and apply schema transformations."""
    if table_code == "vol":
        return _clean_vol(df, target_schema)
    elif table_code == "ngl":
        return _clean_ngl(df, config, target_schema)
    return df


def _clean_vol(df: DataFrame, target_schema) -> DataFrame:
    """Clean conventional data using schema-based transformations."""
    # Apply schema-based transformations
    return _apply_schema_transformations(df, target_schema)


def _clean_ngl(df: DataFrame, config, target_schema) -> DataFrame:
    """Clean NGL data using schema-based transformations."""
    # Apply schema-based transformations first
    df = _apply_schema_transformations(df, target_schema)

    # Ensure production values are non-negative
    production_cols = config.ngl.production_columns
    for col_name in production_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, when(col(col_name) < 0, lit(0.0)).otherwise(col(col_name))
            )

    return df


def _add_days_from_first(df: DataFrame) -> DataFrame:
    """Add DaysFromFirst column for NGL silver data."""
    # Add days since first production for each well using window functions
    df = df.orderBy("WellID", "ProductionMonth")
    first_production_window = Window.partitionBy("WellID")

    df = (
        df.withColumn(
            "FirstProductionDate",
            spark_min("ProductionMonth").over(first_production_window),
        )
        .withColumn(
            "DaysFromFirst",
            datediff(col("ProductionMonth"), col("FirstProductionDate")).cast(
                IntegerType()
            ),
        )
        .drop("FirstProductionDate")
    )

    return df


def _validate_schema_constraints(df: DataFrame, target_schema) -> DataFrame:
    """Validate and enforce schema constraints, particularly non-nullable fields."""
    # Filter out rows that violate non-nullable constraints
    for field in target_schema.fields:
        if not field.nullable and field.name in df.columns:
            # Remove rows where required fields are null
            df = df.filter(col(field.name).isNotNull())

    return df


def _apply_schema_transformations(df: DataFrame, target_schema) -> DataFrame:
    """Apply schema-based transformations to convert strings to proper types."""
    # Get field definitions from schema
    field_map = {field.name: field for field in target_schema.fields}

    # Skip calculated/specially handled columns that shouldn't be cleaned from raw data
    skip_columns = {"DaysFromFirst", "ProductionMonth"}

    for field_name, field in field_map.items():
        if field_name in df.columns and field_name not in skip_columns:
            if field.dataType == DoubleType():
                # Clean and convert numeric columns
                cleaned_col = regexp_replace(col(field_name), r"[^\d.-]", "")
                df = df.withColumn(
                    field_name,
                    when((cleaned_col == "") | (cleaned_col.isNull()), None).otherwise(
                        cleaned_col.cast(DoubleType())
                    ),
                )
            elif field.dataType == IntegerType():
                # Clean and convert integer columns
                cleaned_col = regexp_replace(col(field_name), r"[^\d-]", "")
                df = df.withColumn(
                    field_name,
                    when((cleaned_col == "") | (cleaned_col.isNull()), None).otherwise(
                        cleaned_col.cast(IntegerType())
                    ),
                )
            elif field.dataType == TimestampType():
                # Handle timestamp columns - if this is ProductionMonth, it should be validated elsewhere
                # For other timestamp fields, try conversion or set to null if invalid
                if field_name == "ProductionMonth":
                    continue  # ProductionMonth is handled specially in silver processing
                else:
                    df = df.withColumn(
                        field_name,
                        when(
                            (col(field_name) == "") | (col(field_name).isNull()), None
                        ).otherwise(to_timestamp(col(field_name))),
                    )
            elif field.dataType == StringType():
                # Clean string columns
                df = df.withColumn(field_name, trim(col(field_name)))

    return df
