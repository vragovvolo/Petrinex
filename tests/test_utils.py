from petrinex.utils import table_exists


def test_table_exists_when_table_exists(spark):
    """Test that table_exists returns True when table exists."""
    # Create a temporary table
    df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
    df.createOrReplaceTempView("test_table")

    # Test with the temporary table
    assert table_exists(spark, "test_table") is True


def test_table_exists_when_table_does_not_exist(spark):
    """Test that table_exists returns False when table does not exist."""
    non_existent_table = "non_existent_database.non_existent_table"

    assert table_exists(spark, non_existent_table) is False
