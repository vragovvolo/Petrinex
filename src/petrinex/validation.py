import logging
from typing import Tuple, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, TimestampType

logger = logging.getLogger(__name__)


def _hours_in_month_col(prod_month_col: str = "ProductionMonth") -> F.Column:
    """
    Compute number of hours in the month of ProductionMonth.
    Uses difference between first day of next month and first day of month.
    """
    first_day = F.date_trunc("month", F.col(prod_month_col))
    next_month_first = F.add_months(first_day, 1)
    days = F.datediff(next_month_first, first_day)
    return (days.cast("double") * F.lit(24.0)).alias("HoursInMonth")


def _get_numeric_columns(df: DataFrame) -> List[str]:
    return [
        f.name
        for f in df.schema.fields
        if isinstance(f.dataType, DoubleType) and f.name != "HoursInMonth"
    ]


def validate_silver(
    df: DataFrame, production_cols: List[str]
) -> Tuple[DataFrame, DataFrame]:
    """
    Validate silver-stage NGL/Conventional data with simple, high-value rules.

    Rules implemented:
    - Fatal: Hours < 0
    - Fatal: Hours == 0 and any production > 0
    - Fatal: Hours > HoursInMonth + 24
    - Warn: Negative volumes are clipped to 0 (recorded as issue)
    - Warn: |Hours - HoursInMonth| > 15% AND any production > 0
    - Warn: Aggregated duplicates by (WellID, ProductionMonth)

    Returns:
      (df_clean, issues_df)
    """
    if "ProductionMonth" not in df.columns or "WellID" not in df.columns:
        logger.warning("validate_silver: missing required columns; skipping validation")
        return df, df.sparkSession.createDataFrame(
            [],
            schema="WellID string, ProductionMonth timestamp, Rule string, Severity string, Field string, OriginalValue string, CorrectedValue string, Note string",
        )

    # Add HoursInMonth helper
    df = df.withColumn("HoursInMonth", _hours_in_month_col("ProductionMonth"))

    # Aggregate duplicates (WellID, ProductionMonth)
    key_cols = ["WellID", "ProductionMonth"]
    numeric_cols = _get_numeric_columns(df)
    string_cols = [
        f.name
        for f in df.schema.fields
        if isinstance(f.dataType, StringType) and f.name not in key_cols
    ]

    dup_counts = df.groupBy(key_cols).count().withColumnRenamed("count", "_dup_count")
    df_with_dups = df.join(dup_counts, on=key_cols, how="left")

    # Aggregate by summing numeric and taking first string
    agg_exprs = {c: F.first(c, ignorenulls=True) for c in string_cols}
    for c in numeric_cols:
        agg_exprs[c] = F.sum(F.col(c))

    aggregated = df_with_dups.groupBy(key_cols).agg(agg_exprs)

    # Flatten struct columns created by agg
    for c in aggregated.columns:
        if c.endswith(")") and "(" in c:
            # Spark sometimes names like 'sum(Hours)'
            base = c.split("(")[1].split(")")[0]
            aggregated = aggregated.withColumnRenamed(c, base)

    # Recompute HoursInMonth after aggregation
    aggregated = aggregated.withColumn(
        "HoursInMonth", _hours_in_month_col("ProductionMonth")
    )

    # Build issues for duplicates
    dup_issues = (
        df_with_dups.filter(F.col("_dup_count") > 1)
        .select(
            F.col("WellID"),
            F.col("ProductionMonth"),
            F.lit("aggregated_duplicate").alias("Rule"),
            F.lit("Warn").alias("Severity"),
            F.lit("").alias("Field"),
            F.col("_dup_count").cast("string").alias("OriginalValue"),
            F.lit("").alias("CorrectedValue"),
            F.lit("Aggregated rows for WellID+ProductionMonth").alias("Note"),
        )
        .distinct()
    )

    # Negative volume clipping issues and apply correction
    issues_list = [dup_issues]
    corrected = aggregated
    for pcol in production_cols:
        if pcol in corrected.columns:
            neg_filter = corrected.filter(F.col(pcol) < 0)
            if neg_filter.head(1):
                issues_list.append(
                    neg_filter.select(
                        "WellID",
                        "ProductionMonth",
                        F.lit("negative_clipped").alias("Rule"),
                        F.lit("Warn").alias("Severity"),
                        F.lit(pcol).alias("Field"),
                        F.col(pcol).cast("string").alias("OriginalValue"),
                        F.lit("0").alias("CorrectedValue"),
                        F.lit("Negative production clipped to 0").alias("Note"),
                    )
                )
            corrected = corrected.withColumn(
                pcol, F.when(F.col(pcol) < 0, F.lit(0.0)).otherwise(F.col(pcol))
            )

    # Fatal conditions
    fatal_hours_negative = corrected.filter(F.col("Hours") < 0).select(
        "WellID",
        "ProductionMonth",
        F.lit("hours_negative").alias("Rule"),
        F.lit("Fatal").alias("Severity"),
        F.lit("Hours").alias("Field"),
        F.col("Hours").cast("string").alias("OriginalValue"),
        F.lit("").alias("CorrectedValue"),
        F.lit("").alias("Note"),
    )

    fatal_hours_zero_with_prod = corrected.filter(
        (F.col("Hours") == 0)
        & (sum([F.when(F.col(c) > 0, 1).otherwise(0) for c in production_cols]) > 0)
    ).select(
        "WellID",
        "ProductionMonth",
        F.lit("hours_zero_with_production").alias("Rule"),
        F.lit("Fatal").alias("Severity"),
        F.lit("Hours").alias("Field"),
        F.col("Hours").cast("string").alias("OriginalValue"),
        F.lit("").alias("CorrectedValue"),
        F.lit("").alias("Note"),
    )

    fatal_hours_too_high = corrected.filter(
        F.col("Hours") > (F.col("HoursInMonth") + F.lit(24.0))
    ).select(
        "WellID",
        "ProductionMonth",
        F.lit("hours_exceed_month_by_over_24").alias("Rule"),
        F.lit("Fatal").alias("Severity"),
        F.lit("Hours").alias("Field"),
        F.col("Hours").cast("string").alias("OriginalValue"),
        F.lit("").alias("CorrectedValue"),
        F.lit("").alias("Note"),
    )

    # Warn: hours deviation > 15% and any production > 0
    warn_hours_mismatch = corrected.filter(
        (F.col("HoursInMonth") > 0)
        & (
            (F.abs(F.col("Hours") - F.col("HoursInMonth")) / F.col("HoursInMonth"))
            > F.lit(0.15)
        )
        & (sum([F.when(F.col(c) > 0, 1).otherwise(0) for c in production_cols]) > 0)
    ).select(
        "WellID",
        "ProductionMonth",
        F.lit("hours_significantly_mismatch_month").alias("Rule"),
        F.lit("Warn").alias("Severity"),
        F.lit("Hours").alias("Field"),
        F.col("Hours").cast("string").alias("OriginalValue"),
        F.col("HoursInMonth").cast("string").alias("CorrectedValue"),
        F.lit("Check downtime/reporting").alias("Note"),
    )

    issues_list.extend(
        [
            fatal_hours_negative,
            fatal_hours_zero_with_prod,
            fatal_hours_too_high,
            warn_hours_mismatch,
        ]
    )

    issues_df = None
    for isu in issues_list:
        if issues_df is None:
            issues_df = isu
        else:
            issues_df = issues_df.unionByName(isu, allowMissingColumns=True)

    if issues_df is None:
        issues_df = df.sparkSession.createDataFrame(
            [],
            schema="WellID string, ProductionMonth timestamp, Rule string, Severity string, Field string, OriginalValue string, CorrectedValue string, Note string",
        )

    # Drop fatal rows
    fatal_keys = (
        issues_df.filter(F.col("Severity") == "Fatal")
        .select("WellID", "ProductionMonth")
        .distinct()
    )
    cleaned = corrected.join(
        fatal_keys, on=["WellID", "ProductionMonth"], how="left_anti"
    )

    # Remove helper column
    cleaned = cleaned.drop("HoursInMonth")

    return cleaned, issues_df


def backfill_calendar_months(calendar_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Backfill missing months per well between min and max ProductionMonth.
    Missing months are filled with zeros for numeric production and Hours; add info issue per backfilled row.
    """
    key_cols = ["WellID", "ProductionMonth"]

    # Determine min/max per well
    rng = calendar_df.groupBy("WellID").agg(
        F.min("ProductionMonth").alias("min_pm"),
        F.max("ProductionMonth").alias("max_pm"),
    )

    # Generate full month sequences
    seq_df = (
        rng.select(
            "WellID",
            F.explode(
                F.sequence(
                    F.to_date(F.col("min_pm")),
                    F.to_date(F.col("max_pm")),
                    F.expr("interval 1 month"),
                )
            ).alias("pm_date"),
        )
        .withColumn("ProductionMonth", F.to_timestamp(F.col("pm_date")))
        .drop("pm_date")
    )

    # Left join to find missing months
    full_calendar = seq_df.join(calendar_df, on=key_cols, how="left")

    # Fill numeric with zeros where missing
    numeric_cols = _get_numeric_columns(full_calendar)
    filled = full_calendar
    for c in numeric_cols:
        filled = filled.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))

    # Build backfill issues for rows that were missing
    backfill_issues = filled.filter(
        sum([F.when(F.col(c).isNull(), 1).otherwise(0) for c in calendar_df.columns])
        > 0
    ).select(
        "WellID",
        "ProductionMonth",
        F.lit("backfilled_month").alias("Rule"),
        F.lit("Info").alias("Severity"),
        F.lit("").alias("Field"),
        F.lit("").alias("OriginalValue"),
        F.lit("").alias("CorrectedValue"),
        F.lit("Inserted missing month with zero volumes/hours").alias("Note"),
    )

    return filled, backfill_issues


def validate_calendar(calendar_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Validate calendar data for monotonicity and completeness; backfill missing months.

    Returns:
      (df_clean, issues_df)
    """
    # Backfill missing months
    filled, backfill_issues = backfill_calendar_months(calendar_df)

    # Monotonicity checks per well
    w = Window.partitionBy("WellID").orderBy("ProductionMonth")
    with_prev = (
        filled.withColumn("prev_hours_cum", F.lag("HoursCumulative").over(w))
        .withColumn("prev_gas_cum", F.lag("GasProductionCumulative").over(w))
        .withColumn("prev_oil_cum", F.lag("OilProductionCumulative").over(w))
        .withColumn("prev_cond_cum", F.lag("CondensateProductionCumulative").over(w))
    )

    fatal_hours_cum_decrease = with_prev.filter(
        F.col("prev_hours_cum").isNotNull()
        & (F.col("HoursCumulative") < F.col("prev_hours_cum"))
    ).select(
        "WellID",
        "ProductionMonth",
        F.lit("hours_cumulative_decreased").alias("Rule"),
        F.lit("Fatal").alias("Severity"),
        F.lit("HoursCumulative").alias("Field"),
        F.col("prev_hours_cum").cast("string").alias("OriginalValue"),
        F.col("HoursCumulative").cast("string").alias("CorrectedValue"),
        F.lit("").alias("Note"),
    )

    warn_prod_cum_decrease = with_prev.filter(
        (
            (
                F.col("prev_gas_cum").isNotNull()
                & (F.col("GasProductionCumulative") < F.col("prev_gas_cum"))
            )
            | (
                F.col("prev_oil_cum").isNotNull()
                & (F.col("OilProductionCumulative") < F.col("prev_oil_cum"))
            )
            | (
                F.col("prev_cond_cum").isNotNull()
                & (F.col("CondensateProductionCumulative") < F.col("prev_cond_cum"))
            )
        )
    ).select(
        "WellID",
        "ProductionMonth",
        F.lit("production_cumulative_decreased").alias("Rule"),
        F.lit("Warn").alias("Severity"),
        F.lit("").alias("Field"),
        F.lit("").alias("OriginalValue"),
        F.lit("").alias("CorrectedValue"),
        F.lit("Check source data or downtime corrections").alias("Note"),
    )

    issues_df = backfill_issues.unionByName(
        fatal_hours_cum_decrease, allowMissingColumns=True
    ).unionByName(warn_prod_cum_decrease, allowMissingColumns=True)

    # Drop fatal keys
    fatal_keys = (
        issues_df.filter(F.col("Severity") == "Fatal")
        .select("WellID", "ProductionMonth")
        .distinct()
    )
    cleaned = filled.join(fatal_keys, on=["WellID", "ProductionMonth"], how="left_anti")

    return cleaned, issues_df
