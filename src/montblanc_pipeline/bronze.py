import logging
import requests
import json
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType, TimestampType
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from montblanc_pipeline.config import WAYPOINTS, API_BASE_URL, VARIABLES, START_DATE, END_DATE, END_DATE_ACTIVE, BRONZE_WEATHER_RAW, LAG_DAYS
from montblanc_pipeline.utils import get_watermark, update_watermark, write_delta_table

logger = logging.getLogger(__name__)


def generate_months(start: date, end: date) -> list[tuple]:
    months = []
    current = start
    while current <= end:
        period_end = (current.replace(day=1) + relativedelta(months=1)) - relativedelta(days=1)
        months.append((current, min(period_end, end)))
        current = period_end + relativedelta(days=1)
    return months


def fetch_waypoint(waypoint_name: str, waypoint_config: dict, start: date, end: date) -> dict:
    params = {
        "latitude": waypoint_config["lat"],
        "longitude": waypoint_config["lon"],
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "daily": ",".join(VARIABLES),
        "timezone": "Europe/Paris"
    }

    for attempt in range(3):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            if attempt == 2:
                raise
            logger.warning("Attempt %d failed for %s %s, retrying...", attempt + 1, waypoint_name, start)


def transform_bronze(raw_data: list[dict]) -> "DataFrame":
    rows = []
    for entry in raw_data:
        rows.append((
            entry["waypoint_name"],
            entry["elevation"],
            entry["period_start"],
            entry["period_end"],
            json.dumps(entry["response"]),
            datetime.now()
        ))

    schema = StructType([
        StructField("waypoint_name", StringType(), False),
        StructField("elevation", IntegerType(), False),
        StructField("period_start", DateType(), False),
        StructField("period_end", DateType(), False),
        StructField("response_json", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False)
    ])

    return spark.createDataFrame(rows, schema)


def load_bronze() -> None:
    latest = get_watermark("bronze")
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    start = latest + relativedelta(days=1) if latest != date(1900, 1, 1) else start_date
    end = date.today() - timedelta(days=LAG_DAYS) if not END_DATE_ACTIVE else datetime.strptime(END_DATE, "%Y-%m-%d").date()

    months = generate_months(start, end)

    for period_start, period_end in months:
        raw_data = []
        for waypoint_name, waypoint_config in WAYPOINTS.items():
            logger.info("Fetching %s for %s to %s...", waypoint_name, period_start, period_end)
            response = fetch_waypoint(waypoint_name, waypoint_config, period_start, period_end)
            raw_data.append({
                "waypoint_name": waypoint_name,
                "elevation": waypoint_config["elevation"],
                "period_start": period_start,
                "period_end": period_end,
                "response": response
            })

        df = transform_bronze(raw_data)
        write_delta_table(df, BRONZE_WEATHER_RAW)
        update_watermark("bronze", period_end)
        logger.info("Completed %s to %s.", period_start, period_end)

    logger.info("Bronze load complete.")
