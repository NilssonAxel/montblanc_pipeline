from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType

from montblanc_pipeline.utils import get_max_date


def _date_df(spark: SparkSession, dates: list[date | None]):
    schema = StructType([StructField("date", DateType(), True)])
    return spark.createDataFrame([(d,) for d in dates], schema)


def test_get_max_date_returns_max(spark: SparkSession):
    """Returns the latest date when multiple rows are present."""
    df = _date_df(spark, [date(2024, 1, 1), date(2024, 3, 15), date(2024, 2, 10)])
    assert get_max_date(df) == date(2024, 3, 15)


def test_get_max_date_single_row(spark: SparkSession):
    """Returns the only date when there is one row."""
    df = _date_df(spark, [date(2024, 6, 1)])
    assert get_max_date(df) == date(2024, 6, 1)


def test_get_max_date_empty_returns_none(spark: SparkSession):
    """Returns None when the DataFrame is empty."""
    df = _date_df(spark, [])
    assert get_max_date(df) is None


def test_get_max_date_ignores_nulls(spark: SparkSession):
    """Nulls are ignored and the max of non-null dates is returned."""
    df = _date_df(spark, [None, date(2024, 5, 20), None])
    assert get_max_date(df) == date(2024, 5, 20)
