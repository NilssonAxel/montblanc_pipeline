import os

CATALOG = os.getenv("CATALOG_NAME", "montblanc_dev")

WAYPOINTS = {
    "chamonix": {"lat": 45.9237, "lon": 6.8694, "elevation": 1035},
    "tete_rousse": {"lat": 45.8549, "lon": 6.8175, "elevation": 3167},
    "gouter": {"lat": 45.8510, "lon": 6.8300, "elevation": 3835},
    "vallot": {"lat": 45.8393, "lon": 6.8520, "elevation": 4362},
    "summit": {"lat": 45.8326, "lon": 6.8651, "elevation": 4808}
}

API_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "windspeed_10m_max",
    "windgusts_10m_max",
    "precipitation_sum",
    "snowfall_sum",
    "snow_depth_max",
    "surface_pressure_mean",
    "cloudcover_mean",
    "daylight_duration",
    "sunshine_duration"
]

START_DATE = os.getenv("START_DATE", "2020-01-01")
END_DATE = os.getenv("END_DATE", "2020-02-28")
END_DATE_ACTIVE = os.getenv("END_DATE_ACTIVE", "true").lower() == "true"
LAG_DAYS = 7
MAX_STALENESS_DAYS = 14

