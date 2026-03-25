{{ config(
    materialized='incremental',
    unique_key='date'
) }}

WITH summit AS (
    SELECT
        date,
        temperature_min_c,
        windgusts_max_ms,
        cloudcover_pct,
        air_density_kgm3,
        daylight_hours
    FROM {{ source('silver', 'weather') }}
    WHERE waypoint_name = 'summit'
),

gouter AS (
    SELECT
        date,
        temperature_min_c,
        temperature_max_c,
        windgusts_max_ms
    FROM {{ source('silver', 'weather') }}
    WHERE waypoint_name = 'gouter'
),

vallot AS (
    SELECT
        date,
        windgusts_max_ms
    FROM {{ source('silver', 'weather') }}
    WHERE waypoint_name = 'vallot'
),

route_conditions AS (
    -- Aggregate precipitation and snowfall across all waypoints on the route
    SELECT
        date,
        SUM(precipitation_mm) AS total_precipitation_mm,
        SUM(snowfall_cm)      AS total_snowfall_cm
    FROM {{ source('silver', 'weather') }}
    GROUP BY date
),

rolling AS (
    -- Rolling 3-day windows for thaw history and avalanche risk
    SELECT
        g.date,
        AVG(g.temperature_max_c) OVER (ORDER BY g.date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_3day_gouter_temp_max_c,
        SUM(r.total_snowfall_cm) OVER (ORDER BY g.date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3day_snowfall_cm
    FROM gouter g
    JOIN route_conditions r ON g.date = r.date
),

combined AS (
    SELECT
        s.date,
        s.temperature_min_c                 AS summit_temperature_min_c,
        s.windgusts_max_ms                  AS summit_windgusts_max_ms,
        s.cloudcover_pct                    AS summit_cloudcover_pct,
        g.temperature_min_c                 AS gouter_temperature_min_c,
        g.temperature_max_c                 AS gouter_temperature_max_c,
        g.windgusts_max_ms                  AS gouter_windgusts_max_ms,
        v.windgusts_max_ms                  AS vallot_windgusts_max_ms,
        r.total_precipitation_mm,
        r.total_snowfall_cm,
        ro.rolling_3day_snowfall_cm,
        ro.avg_3day_gouter_temp_max_c,
        s.air_density_kgm3                  AS summit_air_density_kgm3,
        s.daylight_hours                    AS summit_daylight_hours
    FROM summit s
    JOIN gouter g  ON s.date = g.date
    JOIN vallot v  ON s.date = v.date
    JOIN route_conditions r  ON s.date = r.date
    JOIN rolling ro ON s.date = ro.date
),

scored AS (
    -- Compute individual sub-scores on a 0-100 scale using linear interpolation
    -- between the ideal threshold (100) and the hard safety limit (0)
    SELECT
        *,
        -- Wind sub-scores: viable ≤ 14 m/s at summit, ≤ 18 m/s at gouter/vallot; ideal ≤ 7 m/s at summit, ≤ 9 m/s at gouter/vallot
        GREATEST(0, LEAST(100, ROUND(100 * (14 - summit_windgusts_max_ms) / 7.0)))     AS summit_wind_score,
        GREATEST(0, LEAST(100, ROUND(100 * (18 - gouter_windgusts_max_ms) / 9.0)))     AS gouter_wind_score,
        GREATEST(0, LEAST(100, ROUND(100 * (18 - vallot_windgusts_max_ms) / 9.0)))     AS vallot_wind_score,
        -- Temperature sub-scores: viable -20°C to +2°C at summit; peaks at -5°C, above 0°C risks thaw at summit snowfields
        GREATEST(0, LEAST(
            ROUND(100 * (summit_temperature_min_c + 20) / 15.0),                        -- lower bound: 0 at -20°C, 100 at -5°C
            ROUND(100 * (2 - summit_temperature_min_c) / 7.0)                           -- upper bound: 100 at -5°C, 0 at +2°C
        ))                                                                               AS summit_temp_score,
        -- Grand Couloir freeze score: viable < 2°C; ideal < -5°C to ensure route stays frozen on ascent and descent
        GREATEST(0, LEAST(100, ROUND(100 * (2 - gouter_temperature_max_c) / 7.0)))     AS gouter_freeze_score,
        -- Visibility score: viable ≤ 60% cloud cover; ideal ≤ 20% — whiteout risk on featureless summit snowfields
        GREATEST(0, LEAST(100, ROUND(100 * (60 - summit_cloudcover_pct) / 40.0)))      AS visibility_score,
        -- Stability sub-scores
        -- Binary: no tolerance for any snowfall in the 3-day window — even small amounts significantly increase avalanche risk
        CASE WHEN rolling_3day_snowfall_cm = 0 THEN 100 ELSE 0 END                     AS snowfall_score,
        -- Thaw history score: viable avg < 0°C; ideal < -5°C — ensures the mountain has remained frozen recently
        GREATEST(0, LEAST(100, ROUND(100 * (-avg_3day_gouter_temp_max_c) / 5.0)))      AS thaw_score,
        -- Viability: all hard safety thresholds must be met
        summit_windgusts_max_ms     <= 14
            AND summit_temperature_min_c >= -20
            AND summit_cloudcover_pct    <= 60
            AND gouter_temperature_min_c  < 0
            AND gouter_temperature_max_c  < 2
            AND avg_3day_gouter_temp_max_c < 0
            AND gouter_windgusts_max_ms  <= 18
            AND vallot_windgusts_max_ms  <= 18
            AND total_precipitation_mm    = 0
            AND total_snowfall_cm         = 0
            AND rolling_3day_snowfall_cm  = 0                                           AS is_viable
    FROM combined
),

category_scores AS (
    -- Weighted category scores
    SELECT
        *,
        ROUND(0.6 * summit_wind_score  + 0.2 * gouter_wind_score   + 0.2 * vallot_wind_score)  AS wind_score,
        ROUND(0.4 * summit_temp_score  + 0.6 * gouter_freeze_score)                             AS temperature_score,
        ROUND(0.6 * snowfall_score     + 0.4 * thaw_score)                                      AS stability_score
    FROM scored
)

SELECT
    date,
    summit_temperature_min_c,
    summit_windgusts_max_ms,
    summit_cloudcover_pct,
    gouter_temperature_min_c,
    gouter_temperature_max_c,
    gouter_windgusts_max_ms,
    vallot_windgusts_max_ms,
    CAST(total_precipitation_mm      AS DECIMAL(10,2)) AS total_precipitation_mm,
    CAST(total_snowfall_cm           AS DECIMAL(10,2)) AS total_snowfall_cm,
    CAST(rolling_3day_snowfall_cm    AS DECIMAL(10,2)) AS rolling_3day_snowfall_cm,
    CAST(avg_3day_gouter_temp_max_c  AS DECIMAL(10,2)) AS avg_3day_gouter_temp_max_c,
    summit_air_density_kgm3,
    summit_daylight_hours,
    CAST(summit_wind_score           AS INT)            AS summit_wind_score,
    CAST(gouter_wind_score           AS INT)            AS gouter_wind_score,
    CAST(vallot_wind_score           AS INT)            AS vallot_wind_score,
    CAST(wind_score                  AS INT)            AS wind_score,
    CAST(summit_temp_score           AS INT)            AS summit_temp_score,
    CAST(gouter_freeze_score         AS INT)            AS gouter_freeze_score,
    CAST(temperature_score           AS INT)            AS temperature_score,
    CAST(visibility_score            AS INT)            AS visibility_score,
    CAST(snowfall_score              AS INT)            AS snowfall_score,
    CAST(thaw_score                  AS INT)            AS thaw_score,
    CAST(stability_score             AS INT)            AS stability_score,
    CAST(ROUND(
        0.30 * wind_score +
        0.25 * temperature_score +
        0.20 * visibility_score +
        0.25 * stability_score
    )                                AS INT)            AS overall_score,
    is_viable,
    is_viable
        AND wind_score        >= 75
        AND temperature_score >= 75
        AND visibility_score  >= 75
        AND stability_score   >= 75                     AS is_ideal
FROM category_scores
{% if is_incremental() %}
-- Look back 3 days to ensure rolling windows have sufficient data for boundary rows
WHERE date > (SELECT MAX(date) FROM {{ this }}) - INTERVAL 3 DAYS
{% endif %}
