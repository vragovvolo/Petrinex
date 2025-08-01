import pytest
from databricks.connect import DatabricksSession as SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder.serverless(True).getOrCreate()

    yield spark

    spark.stop()
