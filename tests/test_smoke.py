"""
Smoke tests — run after deploying to prod.
Lightweight checks that prod tables exist and data is fresh.
"""
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from montblanc_pipeline.config import CATALOG, MAX_STALENESS_DAYS


def test_prod_silver_has_recent_data(spark: SparkSession):
    """Silver weather table has data within the staleness threshold."""
    df = spark.table(f"{CATALOG}.silver.weather")
    max_date = df.agg(F.max("date")).collect()[0][0]
    assert max_date is not None, "silver.weather is empty"
    assert max_date >= date.today() - timedelta(days=MAX_STALENESS_DAYS), \
        f"Data is stale — latest date is {max_date}"


def test_prod_gold_tables_have_rows(spark: SparkSession):
    """All gold tables exist and have rows in prod."""
    for table in [
        "daily_conditions",
        "monthly_conditions",
        "summit_window",
        "elevation_gradient",
        "seasonal_summary",
    ]:
        df = spark.table(f"{CATALOG}.gold.{table}")
        assert df.count() > 0, f"gold.{table} is empty in prod"


def test_prod_daily_conditions_has_recent_data(spark: SparkSession):
    """daily_conditions has recent data — indicates dbt ran successfully."""
    df = spark.table(f"{CATALOG}.gold.daily_conditions")
    max_date = df.agg(F.max("date")).collect()[0][0]
    assert max_date is not None, "daily_conditions is empty"
    assert max_date >= date.today() - timedelta(days=MAX_STALENESS_DAYS), \
        f"daily_conditions data is stale — latest date is {max_date}"


def test_prod_summit_window_has_recent_data(spark: SparkSession):
    """summit_window has recent data — indicates scoring logic ran successfully."""
    df = spark.table(f"{CATALOG}.gold.summit_window")
    max_date = df.agg(F.max("date")).collect()[0][0]
    assert max_date is not None, "summit_window is empty"
    assert max_date >= date.today() - timedelta(days=MAX_STALENESS_DAYS), \
        f"summit_window data is stale — latest date is {max_date}"
