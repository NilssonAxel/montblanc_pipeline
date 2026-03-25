{{ config(
    materialized='incremental',
    unique_key=['waypoint_name', 'date']
) }}

SELECT
    waypoint_name,
    elevation,
    date,
    temperature_max_c,
    temperature_min_c,
    windspeed_max_ms,
    windgusts_max_ms,
    precipitation_mm,
    snowfall_cm,
    snow_depth_m,
    pressure_hpa,
    cloudcover_pct,
    daylight_hours,
    sunshine_hours,
    air_density_kgm3
FROM {{ source('silver', 'weather') }}
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}