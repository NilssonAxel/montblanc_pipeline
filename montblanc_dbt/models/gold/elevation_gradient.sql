{{ config(
    materialized='incremental',
    unique_key=['date', 'waypoint_name']
) }}

WITH conditions AS (
    SELECT
        date,
        waypoint_name,
        elevation,
        temperature_max_c,
        temperature_min_c,
        windspeed_max_ms,
        windgusts_max_ms,
        air_density_kgm3,
        snow_depth_m,
        cloudcover_pct
    FROM {{ source('silver', 'weather') }}
    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

with_previous AS (
    -- Attach the previous waypoint's values (ordered by elevation) for gradient calculation
    SELECT
        *,
        LAG(elevation)        OVER (PARTITION BY date ORDER BY elevation) AS prev_elevation,
        LAG(temperature_max_c) OVER (PARTITION BY date ORDER BY elevation) AS prev_temperature_max_c,
        LAG(temperature_min_c) OVER (PARTITION BY date ORDER BY elevation) AS prev_temperature_min_c,
        LAG(windspeed_max_ms)  OVER (PARTITION BY date ORDER BY elevation) AS prev_windspeed_max_ms,
        LAG(windgusts_max_ms)  OVER (PARTITION BY date ORDER BY elevation) AS prev_windgusts_max_ms,
        LAG(air_density_kgm3)  OVER (PARTITION BY date ORDER BY elevation) AS prev_air_density_kgm3,
        LAG(snow_depth_m)      OVER (PARTITION BY date ORDER BY elevation) AS prev_snow_depth_m
    FROM conditions
)

SELECT
    date,
    waypoint_name,
    elevation,
    -- Rate of change per 100m elevation gain from the waypoint below
    -- Null for chamonix (lowest waypoint) as there is no waypoint below
    CAST((temperature_max_c - prev_temperature_max_c) / NULLIF(elevation - prev_elevation, 0) * 100       AS DECIMAL(10,2)) AS temp_max_per_100m,
    CAST((temperature_min_c - prev_temperature_min_c) / NULLIF(elevation - prev_elevation, 0) * 100       AS DECIMAL(10,2)) AS temp_min_per_100m,
    CAST((windspeed_max_ms  - prev_windspeed_max_ms)  / NULLIF(elevation - prev_elevation, 0) * 100       AS DECIMAL(10,2)) AS windspeed_per_100m,
    CAST((windgusts_max_ms  - prev_windgusts_max_ms)  / NULLIF(elevation - prev_elevation, 0) * 100       AS DECIMAL(10,2)) AS windgusts_per_100m,
    CAST((air_density_kgm3  - prev_air_density_kgm3)  / NULLIF(elevation - prev_elevation, 0) * 1000      AS DECIMAL(10,2)) AS air_density_per_1000m,
    CAST((snow_depth_m      - prev_snow_depth_m)       / NULLIF(elevation - prev_elevation, 0) * 100 * 100 AS DECIMAL(10,2)) AS snow_depth_cm_per_100m
FROM with_previous
