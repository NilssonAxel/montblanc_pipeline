{{ config(
    materialized='table'
) }}

WITH summit_conditions AS (
    SELECT
        MONTH(date)               AS month,
        DATE_FORMAT(date, 'MMMM') AS month_name,
        temperature_min_c,
        temperature_max_c,
        windgusts_max_ms,
        cloudcover_pct,
        snow_depth_m,
        air_density_kgm3,
        daylight_hours
    FROM {{ source('silver', 'weather') }}
    WHERE waypoint_name = 'summit'
),

summit_window AS (
    SELECT
        MONTH(date)  AS month,
        is_viable,
        is_ideal,
        overall_score
    FROM {{ ref('summit_window') }}
)

SELECT
    sc.month,
    sc.month_name,
    CAST(AVG(sc.temperature_min_c)  AS DECIMAL(10,2)) AS avg_temperature_min_c,
    CAST(AVG(sc.temperature_max_c)  AS DECIMAL(10,2)) AS avg_temperature_max_c,
    CAST(AVG(sc.windgusts_max_ms)   AS DECIMAL(10,2)) AS avg_windgusts_max_ms,
    CAST(AVG(sc.cloudcover_pct)     AS DECIMAL(10,2)) AS avg_cloudcover_pct,
    CAST(AVG(sc.snow_depth_m)       AS DECIMAL(10,2)) AS avg_snow_depth_m,
    CAST(AVG(sc.air_density_kgm3)   AS DECIMAL(10,2)) AS avg_air_density_kgm3,
    CAST(AVG(sc.daylight_hours)     AS DECIMAL(10,2)) AS avg_daylight_hours,
    CAST(AVG(sw.overall_score)      AS DECIMAL(10,2)) AS avg_overall_score,
    CAST(100.0 * SUM(CASE WHEN sw.is_viable THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS viable_days_pct,
    CAST(100.0 * SUM(CASE WHEN sw.is_ideal  THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(10,2)) AS ideal_days_pct
FROM summit_conditions sc
JOIN summit_window sw ON sc.month = sw.month
GROUP BY sc.month, sc.month_name
ORDER BY sc.month
