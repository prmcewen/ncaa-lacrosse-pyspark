import pytest
from pyspark.sql import SparkSession

from src.etl.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Shared session-scoped SparkSession across all test modules."""
    s = get_spark_session("Pytest-Shared-Spark")
    yield s
    s.stop()
