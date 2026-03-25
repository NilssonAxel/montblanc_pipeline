{{ config(
    materialized='incremental',
    unique_key=['waypoint_name', 'year_month']
) }}

SELECT
    waypoint_name,
    elevation,
    YEAR(date)                          AS year,
    DATE_TRUNC('month', date)           AS year_month,
    DATE_FORMAT(date, 'MMMM')           AS month_name,
    CAST(AVG(temperature_max_c)         AS DECIMAL(10,2)) AS avg_temperature_max_c,
    CAST(AVG(temperature_min_c)         AS DECIMAL(10,2)) AS avg_temperature_min_c,
    CAST(AVG(windspeed_max_ms)          AS DECIMAL(10,2)) AS avg_windspeed_max_ms,
    CAST(AVG(windgusts_max_ms)          AS DECIMAL(10,2)) AS avg_windgusts_max_ms,
    CAST(AVG(precipitation_mm)          AS DECIMAL(10,2)) AS avg_precipitation_mm,
    CAST(AVG(snowfall_cm)               AS DECIMAL(10,2)) AS avg_snowfall_cm,
    CAST(AVG(snow_depth_m)              AS DECIMAL(10,2)) AS avg_snow_depth_m,
    CAST(AVG(pressure_hpa)              AS DECIMAL(10,2)) AS avg_pressure_hpa,
    CAST(AVG(cloudcover_pct)            AS DECIMAL(10,2)) AS avg_cloudcover_pct,
    CAST(AVG(daylight_hours)            AS DECIMAL(10,2)) AS avg_daylight_hours,
    CAST(AVG(sunshine_hours)            AS DECIMAL(10,2)) AS avg_sunshine_hours,
    CAST(AVG(air_density_kgm3)          AS DECIMAL(10,2)) AS avg_air_density_kgm3
FROM {{ source('silver', 'weather') }}
{% if is_incremental() %}
WHERE DATE_TRUNC('month', date) > (
    SELECT COALESCE(MAX(year_month), '1900-01-01')
    FROM {{ this }}
)
AND DATE_TRUNC('month', date) < DATE_TRUNC('month', CURRENT_DATE()) -- exclude current month as it is incomplete
{% endif %}
GROUP BY waypoint_name, elevation, YEAR(date), DATE_TRUNC('month', date), DATE_FORMAT(date, 'MMMM')