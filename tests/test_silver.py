import json
import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

from montblanc_pipeline.silver import transform_silver


def _bronze_schema() -> StructType:
    return StructType([
        StructField("waypoint_name", StringType(), False),
        StructField("elevation", IntegerType(), False),
        StructField("period_end", DateType(), False),
        StructField("response_json", StringType(), False),
    ])


def _make_row(waypoint_name: str, elevation: str, period_end: date, daily: dict) -> tuple:
    return (waypoint_name, elevation, period_end, json.dumps({"daily": daily}))


def _default_daily() -> dict:
    return {
        "time": ["2024-01-15"],
        "temperature_2m_max": [5.0],
        "temperature_2m_min": [-2.0],
        "windspeed_10m_max": [36.0],
        "windgusts_10m_max": [72.0],
        "precipitation_sum": [1.5],
        "snowfall_sum": [0.5],
        "snow_depth_max": [0.3],
        "surface_pressure_mean": [1013.0],
        "cloudcover_mean": [75.0],
        "daylight_duration": [36000.0],
        "sunshine_duration": [18000.0],
    }


def test_output_schema(spark: SparkSession):
    """transform_silver produces exactly the expected output columns."""
    df = spark.createDataFrame(
        [_make_row("Summit", 4808, date(2024, 1, 31), _default_daily())],
        _bronze_schema(),
    )
    result = transform_silver(df)

    expected_cols = {
        "waypoint_name", "elevation", "date",
        "temperature_max_c", "temperature_min_c",
        "windspeed_max_ms", "windgusts_max_ms",
        "precipitation_mm", "snowfall_cm", "snow_depth_m",
        "pressure_hpa", "cloudcover_pct",
        "daylight_hours", "sunshine_hours", "air_density_kgm3",
    }
    assert set(result.columns) == expected_cols



def test_windspeed_conversion(spark: SparkSession):
    """Windspeed and windgusts are converted from km/h to m/s (divide by 3.6)."""
    daily = {**_default_daily(), "windspeed_10m_max": [36.0], "windgusts_10m_max": [72.0]}
    df = spark.createDataFrame(
        [_make_row("Summit", 4808, date(2024, 1, 31), daily)],
        _bronze_schema(),
    )
    row = transform_silver(df).collect()[0]

    assert float(row.windspeed_max_ms) == pytest.approx(10.0, abs=0.01)
    assert float(row.windgusts_max_ms) == pytest.approx(20.0, abs=0.01)


def test_duration_conversion(spark: SparkSession):
    """Daylight and sunshine durations are converted from seconds to hours (divide by 3600)."""
    daily = {**_default_daily(), "daylight_duration": [36000.0], "sunshine_duration": [18000.0]}
    df = spark.createDataFrame(
        [_make_row("Summit", 4808, date(2024, 1, 31), daily)],
        _bronze_schema(),
    )
    row = transform_silver(df).collect()[0]

    assert float(row.daylight_hours) == pytest.approx(10.0, abs=0.01)
    assert float(row.sunshine_hours) == pytest.approx(5.0, abs=0.01)


def test_air_density_calculation(spark: SparkSession):
    """Air density is derived using ρ = P(Pa) / (287.05 × T(K))."""
    daily = {**_default_daily(), "surface_pressure_mean": [1013.25], "temperature_2m_min": [0.0]}
    df = spark.createDataFrame(
        [_make_row("Summit", 4808, date(2024, 1, 31), daily)],
        _bronze_schema(),
    )
    row = transform_silver(df).collect()[0]

    expected = (1013.25 * 100) / (287.05 * (0.0 + 273.15))
    assert float(row.air_density_kgm3) == pytest.approx(expected, abs=0.01)


def test_deduplication_keeps_latest_period_end(spark: SparkSession):
    """When the same waypoint+date appears across multiple months, keep the most recent period_end."""
    daily = _default_daily()  # both rows have the same date: 2024-01-15
    older = _make_row("Summit", 4808, date(2024, 1, 31), daily)
    newer = _make_row("Summit", 4808, date(2024, 2, 29), daily)

    df = spark.createDataFrame([older, newer], _bronze_schema())
    result = transform_silver(df)

    assert result.count() == 1


def test_multiple_days_exploded(spark: SparkSession):
    """Each day in the daily arrays becomes its own row."""
    daily = {
        "time": ["2024-01-01", "2024-01-02"],
        "temperature_2m_max": [3.0, 4.0],
        "temperature_2m_min": [-1.0, -2.0],
        "windspeed_10m_max": [10.0, 20.0],
        "windgusts_10m_max": [15.0, 25.0],
        "precipitation_sum": [0.0, 1.0],
        "snowfall_sum": [0.0, 0.5],
        "snow_depth_max": [0.1, 0.2],
        "surface_pressure_mean": [1010.0, 1012.0],
        "cloudcover_mean": [50.0, 80.0],
        "daylight_duration": [28800.0, 29000.0],
        "sunshine_duration": [14400.0, 10000.0],
    }
    df = spark.createDataFrame(
        [_make_row("Summit", 4808, date(2024, 1, 31), daily)],
        _bronze_schema(),
    )
    assert transform_silver(df).count() == 2
