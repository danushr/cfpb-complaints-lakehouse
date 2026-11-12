import pytest

from lakehouse.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests")
    s.conf.set("spark.sql.shuffle.partitions", "2")
    yield s
    s.stop()
