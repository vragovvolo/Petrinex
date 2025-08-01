from pyspark.sql.utils import AnalysisException


def table_exists(spark, full_table_name: str) -> bool:
    try:
        spark.table(full_table_name).limit(1).collect()
        return True
    except AnalysisException:
        return False


def existing_months(
    spark, full_table_name: str, prod_month_col: str = "PRODUCTION_MONTH"
) -> set:
    """
    Return a Python set of distinct production months already present in the table.
    If the table doesn’t exist yet, returns an empty set().
    """
    if not table_exists(spark, full_table_name):
        return set()
    return {
        r[0]
        for r in spark.table(full_table_name)
        .select(prod_month_col)
        .distinct()
        .collect()
    }
