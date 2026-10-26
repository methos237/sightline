from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from pipeline.spark import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = get_spark("sightline-tests")
    yield session
    session.stop()
