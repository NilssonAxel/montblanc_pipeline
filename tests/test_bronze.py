from datetime import date
from unittest.mock import patch, MagicMock

import pytest
import requests
from pyspark.sql import SparkSession

from montblanc_pipeline.bronze import generate_months, transform_bronze, fetch_waypoint


# ---------------------------------------------------------------------------
# generate_months — pure Python, no Spark needed
# ---------------------------------------------------------------------------

def test_generate_months_single_full_month():
    """A range contained within one calendar month returns a single tuple."""
    result = generate_months(date(2024, 1, 1), date(2024, 1, 31))
    assert result == [(date(2024, 1, 1), date(2024, 1, 31))]


def test_generate_months_spans_multiple_months():
    """A multi-month range is split into one tuple per calendar month."""
    result = generate_months(date(2024, 1, 1), date(2024, 3, 31))
    assert result == [
        (date(2024, 1, 1), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),  # 2024 is a leap year
        (date(2024, 3, 1), date(2024, 3, 31)),
    ]


def test_generate_months_partial_end_month():
    """When end falls mid-month the last tuple is clipped to that date."""
    result = generate_months(date(2024, 1, 1), date(2024, 2, 15))
    assert result == [
        (date(2024, 1, 1), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 15)),
    ]


def test_generate_months_start_mid_month():
    """When start falls mid-month the first period begins on that date."""
    result = generate_months(date(2024, 1, 15), date(2024, 2, 29))
    assert result == [
        (date(2024, 1, 15), date(2024, 1, 31)),
        (date(2024, 2, 1), date(2024, 2, 29)),
    ]


def test_generate_months_same_day():
    """Start and end on the same day returns a single one-day tuple."""
    result = generate_months(date(2024, 6, 15), date(2024, 6, 15))
    assert result == [(date(2024, 6, 15), date(2024, 6, 15))]


# ---------------------------------------------------------------------------
# transform_bronze — needs Spark; patches the module-level spark import
# ---------------------------------------------------------------------------

def _raw_data() -> list[dict]:
    return [
        {
            "waypoint_name": "Summit",
            "elevation": 4808,
            "period_start": date(2024, 1, 1),
            "period_end": date(2024, 1, 31),
            "response": {"daily": {"time": ["2024-01-15"], "temperature_2m_max": [5.0]}},
        }
    ]


def test_transform_bronze_schema(spark: SparkSession):
    """transform_bronze produces exactly the expected output columns."""
    with patch("montblanc_pipeline.bronze.spark", spark):
        df = transform_bronze(_raw_data())

    expected_cols = {
        "waypoint_name", "elevation", "period_start", "period_end",
        "response_json", "ingestion_timestamp",
    }
    assert set(df.columns) == expected_cols


def test_transform_bronze_row_count(spark: SparkSession):
    """One row is produced per entry in raw_data."""
    with patch("montblanc_pipeline.bronze.spark", spark):
        df = transform_bronze(_raw_data() * 3)
    assert df.count() == 3


def test_transform_bronze_values(spark: SparkSession):
    """waypoint_name, elevation, and period dates are preserved correctly."""
    with patch("montblanc_pipeline.bronze.spark", spark):
        df = transform_bronze(_raw_data())
    row = df.collect()[0]

    assert row.waypoint_name == "Summit"
    assert row.elevation == 4808
    assert row.period_start == date(2024, 1, 1)
    assert row.period_end == date(2024, 1, 31)


def test_transform_bronze_response_is_json_string(spark: SparkSession):
    """response_json stores the response dict as a JSON string."""
    import json
    with patch("montblanc_pipeline.bronze.spark", spark):
        df = transform_bronze(_raw_data())
    row = df.collect()[0]
    parsed = json.loads(row.response_json)
    assert "daily" in parsed


# ---------------------------------------------------------------------------
# fetch_waypoint — mocks requests.get
# ---------------------------------------------------------------------------

_WAYPOINT_CONFIG = {"lat": 45.83, "lon": 6.87}
_API_RESPONSE = {"daily": {"time": ["2024-01-15"]}}


def test_fetch_waypoint_returns_json_on_success():
    """Returns parsed JSON on a successful first attempt."""
    mock_response = MagicMock()
    mock_response.json.return_value = _API_RESPONSE

    with patch("montblanc_pipeline.bronze.requests.get", return_value=mock_response):
        result = fetch_waypoint("Summit", _WAYPOINT_CONFIG, date(2024, 1, 1), date(2024, 1, 31))

    assert result == _API_RESPONSE


def test_fetch_waypoint_retries_on_failure():
    """Retries after a transient error and returns JSON on the second attempt."""
    mock_response = MagicMock()
    mock_response.json.return_value = _API_RESPONSE

    with patch("montblanc_pipeline.bronze.requests.get", side_effect=[
        requests.exceptions.RequestException("timeout"),
        mock_response,
    ]):
        result = fetch_waypoint("Summit", _WAYPOINT_CONFIG, date(2024, 1, 1), date(2024, 1, 31))

    assert result == _API_RESPONSE


def test_fetch_waypoint_raises_after_three_failures():
    """Raises RequestException when all three attempts fail."""
    with patch("montblanc_pipeline.bronze.requests.get",
               side_effect=requests.exceptions.RequestException("timeout")):
        with pytest.raises(requests.exceptions.RequestException):
            fetch_waypoint("Summit", _WAYPOINT_CONFIG, date(2024, 1, 1), date(2024, 1, 31))
