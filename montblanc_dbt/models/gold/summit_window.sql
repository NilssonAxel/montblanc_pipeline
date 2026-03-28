{{ config(
    materialized='incremental',
    unique_key='date'
) }}

WITH summit AS (
    SELECT
        date,
        temperature_min_c,
        temperature_max_c,
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
        windgusts_max_ms,
        sunshine_hours
    FROM {{ source('silver', 'weather') }}
    WHERE waypoint_name = 'gouter'
),

vallot AS (
    SELECT
        date,
        windgusts_max_ms,
        snow_depth_m
    FROM {{ source('silver', 'weather') }}
    WHERE waypoint_name = 'vallot'
),

snowpack AS (
    -- Average snow depth and sunshine hours across route waypoints (tete_rousse, gouter, vallot)
    SELECT
        date,
        AVG(snow_depth_m)   AS avg_route_snow_depth_m,
        AVG(sunshine_hours) AS avg_route_sunshine_hours
    FROM {{ source('silver', 'weather') }}
    WHERE waypoint_name IN ('tete_rousse', 'gouter', 'vallot')
    GROUP BY date
),

route_conditions AS (
    -- Average precipitation and snowfall across all waypoints on the route
    SELECT
        date,
        AVG(precipitation_mm) AS total_precipitation_mm,
        AVG(snowfall_cm)      AS total_snowfall_cm
    FROM {{ source('silver', 'weather') }}
    GROUP BY date
),

rolling AS (
    -- Rolling windows for grand couloir and avalanche risk assessment
    SELECT
        g.date,
        AVG((g.temperature_max_c + g.temperature_min_c) / 2) OVER (ORDER BY g.date ROWS BETWEEN 2  PRECEDING AND CURRENT ROW) AS avg_3day_gouter_temp_mean_c,
        AVG(g.temperature_max_c)                              OVER (ORDER BY g.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_30day_gouter_temp_max_c,
        SUM(r.total_snowfall_cm)                              OVER (ORDER BY g.date ROWS BETWEEN 2  PRECEDING AND CURRENT ROW) AS rolling_3day_snowfall_cm,
        -- 30-day average snow depth: proxy for how well-protected the ice base has been from heat cycling
        AVG(sp.avg_route_snow_depth_m)                        OVER (ORDER BY g.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_30day_snow_depth_m,
        -- 7-day snow depth change: positive = accumulating, negative = melting (kept for dashboard context)
        sp.avg_route_snow_depth_m - LAG(sp.avg_route_snow_depth_m, 7) OVER (ORDER BY g.date)         AS snow_depth_change_7d
    FROM gouter g
    JOIN route_conditions r ON g.date = r.date
    JOIN snowpack sp         ON g.date = sp.date
),

combined AS (
    SELECT
        s.date,
        s.temperature_min_c                 AS summit_temperature_min_c,
        s.temperature_max_c                 AS summit_temperature_max_c,
        s.windgusts_max_ms                  AS summit_windgusts_max_ms,
        s.cloudcover_pct                    AS summit_cloudcover_pct,
        g.temperature_min_c                 AS gouter_temperature_min_c,
        g.temperature_max_c                 AS gouter_temperature_max_c,
        g.windgusts_max_ms                  AS gouter_windgusts_max_ms,
        v.windgusts_max_ms                  AS vallot_windgusts_max_ms,
        r.total_precipitation_mm,
        r.total_snowfall_cm,
        ro.rolling_3day_snowfall_cm,
        ro.avg_3day_gouter_temp_mean_c,
        ro.avg_30day_gouter_temp_max_c,
        ro.avg_30day_snow_depth_m,
        ro.snow_depth_change_7d,
        sp.avg_route_snow_depth_m,
        sp.avg_route_sunshine_hours,
        s.air_density_kgm3                  AS summit_air_density_kgm3,
        s.daylight_hours                    AS summit_daylight_hours
    FROM summit s
    JOIN gouter g   ON s.date = g.date
    JOIN vallot v   ON s.date = v.date
    JOIN route_conditions r  ON s.date = r.date
    JOIN rolling ro ON s.date = ro.date
    JOIN snowpack sp ON s.date = sp.date
),

scored AS (
    -- Compute individual sub-scores on a 0-100 scale
    SELECT
        *,
        -- Wind sub-scores: viable ≤ 14 m/s at summit, ≤ 18 m/s at gouter/vallot; ideal ≤ 7 m/s at summit, ≤ 9 m/s at gouter/vallot
        GREATEST(0, LEAST(100, ROUND(100 * (14 - summit_windgusts_max_ms) / 7.0)))      AS summit_wind_score,
        GREATEST(0, LEAST(100, ROUND(100 * (18 - gouter_windgusts_max_ms) / 9.0)))      AS gouter_wind_score,
        GREATEST(0, LEAST(100, ROUND(100 * (18 - vallot_windgusts_max_ms) / 9.0)))      AS vallot_wind_score,
        -- Summit temperature sub-scores: based on max temp (proxy for summit conditions at 10am)
        -- Cold score: ideal ≥ -10°C; viable down to -15°C, 0 at -15°C
        CASE
            WHEN summit_temperature_max_c >= -10 THEN 100
            ELSE GREATEST(0, ROUND(100 * (summit_temperature_max_c + 15) / 5.0))
        END                                                                               AS summit_cold_score,
        -- Heat score: ideal ≤ 0°C; thaw risk above 0°C, 0 at +2°C
        CASE
            WHEN summit_temperature_max_c <= 0 THEN 100
            ELSE GREATEST(0, ROUND(100 * (2 - summit_temperature_max_c) / 2.0))
        END                                                                               AS summit_heat_score,
        -- Grand Couloir sub-scores
        -- Freeze score: ideal ≤ +2°C (frozen at dawn even if warming during day); drops to 0 at +7°C
        CASE
            WHEN gouter_temperature_max_c <= 2 THEN 100
            ELSE GREATEST(0, ROUND(100 * (7 - gouter_temperature_max_c) / 5.0))
        END                                                                               AS gouter_freeze_score,
        -- Thaw history score: 3-day mean temp — accounts for overnight refreeze dampening daytime thaw
        -- Ideal < -2°C mean; 0 at +3°C mean
        GREATEST(0, LEAST(100, ROUND(100 * (3 - avg_3day_gouter_temp_mean_c) / 5.0)))  AS thaw_score,
        -- Visibility score: ideal ≤ 30%; viable ≤ 70%; 0 at 80%+
        GREATEST(0, LEAST(100, ROUND(100 * (80 - summit_cloudcover_pct) / 50.0)))       AS visibility_score,
        -- Avalanche risk components (0-100, higher = more dangerous)
        -- Thermal risk: cumulative heat effect on snowpack — 0 at -10°C 30-day avg, 100 at +5°C
        LEAST(100, GREATEST(0, ROUND(100 * (avg_30day_gouter_temp_max_c + 10) / 15.0))) AS thermal_risk,
        -- Solar risk: radiation × temperature interaction — cold temps prevent solar melt regardless of sunshine
        -- 0 at ≤ -10°C gouter max; scales with both sunshine hours and temperature above -10°C
        LEAST(100, GREATEST(0, ROUND(
            (avg_route_sunshine_hours / 6.0) *
            GREATEST(0, (gouter_temperature_max_c + 10) / 10.0) * 100
        )))                                                                                AS solar_risk,
        -- Accumulation instability: new snow loading onto a thermally weakened ice base
        -- Risk is realized when snow returns after a period where ice was directly exposed to summer heat
        -- Thin slabs on hard ice can be more unstable than thick ones (higher stress concentration)
        -- Gate: any detectable snow (≥1cm) on a weakened base = full instability active
        LEAST(100, GREATEST(0, ROUND(
            thermal_risk
            * CASE WHEN avg_route_snow_depth_m >= 0.01 THEN 1.0 ELSE 0 END                -- snow must be present (1cm detection threshold)
            * GREATEST(0, 1.0 - avg_30day_snow_depth_m / 0.5)                             -- ice was exposed over past 30 days
        )))                                                                                AS accumulation_instability,
        -- Degradation instability: solar radiation triggering wet slab slides
        -- Solar radiation saturates snowpack with meltwater, lubricating the base — depth provides the mass
        -- Temperature gate is embedded in solar_risk; old or fresh snow both wet-slide when warmed
        LEAST(100, GREATEST(0, ROUND(
            solar_risk
            * LEAST(1.0, avg_route_snow_depth_m / 2.0)                                    -- needs significant snowpack mass to slide
        )))                                                                                AS degradation_instability,
        -- Wind slab instability: heavy snowfall transported and loaded by strong winds
        -- Independent of thermal/solar paths — can compound with either
        LEAST(100, GREATEST(0, ROUND(
            LEAST(1.0, rolling_3day_snowfall_cm / 30.0)                                    -- 30cm avg snowfall over 3 days = full loading
            * LEAST(1.0, gouter_windgusts_max_ms / 20.0)                                   -- 20 m/s gusts = significant wind transport
            * 100
        )))                                                                                AS wind_slab_instability,
        -- Viable: relaxed thresholds — a novice with a guide could summit safely
        summit_windgusts_max_ms      <= 18
            AND summit_temperature_max_c  BETWEEN -18 AND 2
            AND summit_cloudcover_pct     <= 80
            AND gouter_temperature_min_c  <  0
            AND gouter_windgusts_max_ms   <= 20
            AND vallot_windgusts_max_ms   <= 20
            AND total_precipitation_mm     = 0
            AND total_snowfall_cm          = 0
            AND rolling_3day_snowfall_cm  <= 20                                           AS is_viable,
        -- Recommended: strict thresholds — all safety gates met, Couloir stable
        summit_windgusts_max_ms      <= 14
            AND summit_temperature_max_c  BETWEEN -15 AND 2
            AND summit_cloudcover_pct     <= 70
            AND gouter_temperature_min_c  <  0
            AND avg_3day_gouter_temp_mean_c < 0
            AND gouter_windgusts_max_ms   <= 18
            AND vallot_windgusts_max_ms   <= 18
            AND total_precipitation_mm     = 0
            AND total_snowfall_cm          = 0
            AND rolling_3day_snowfall_cm  <= 15                                           AS is_recommended
    FROM combined
),

category_scores AS (
    SELECT
        *,
        -- Wind: weighted average across summit (60%), gouter (20%), vallot (20%)
        ROUND(0.6 * summit_wind_score + 0.2 * gouter_wind_score + 0.2 * vallot_wind_score) AS wind_score,
        -- Temperature: minimum of cold and heat sub-scores — both directions must be acceptable
        LEAST(summit_cold_score, summit_heat_score)                                          AS temperature_score,
        -- Grand Couloir: freeze score (60%) + thaw history (40%)
        ROUND(0.6 * gouter_freeze_score + 0.4 * thaw_score)                                AS grand_couloir_score,
        -- Avalanche: wind slab compounds on top of dominant thermal/solar path
        GREATEST(0, ROUND(100 - LEAST(100, wind_slab_instability + GREATEST(accumulation_instability, degradation_instability)))) AS avalanche_score
    FROM scored
)

SELECT
    date,

    -- Raw measurements: summit
    summit_temperature_min_c,
    summit_temperature_max_c,
    summit_windgusts_max_ms,
    summit_cloudcover_pct,
    summit_air_density_kgm3,
    summit_daylight_hours,

    -- Raw measurements: route waypoints
    gouter_temperature_min_c,
    gouter_temperature_max_c,
    gouter_windgusts_max_ms,
    vallot_windgusts_max_ms,

    -- Raw measurements: route aggregates
    CAST(total_precipitation_mm         AS DECIMAL(10,2)) AS route_precipitation_mm,
    CAST(total_snowfall_cm              AS DECIMAL(10,2)) AS route_snowfall_cm,
    CAST(rolling_3day_snowfall_cm       AS DECIMAL(10,2)) AS route_3d_snowfall_cm,
    CAST(avg_route_snow_depth_m         AS DECIMAL(10,3)) AS route_snow_depth_m,
    CAST(avg_30day_snow_depth_m         AS DECIMAL(10,3)) AS route_30d_snow_depth_m,
    CAST(snow_depth_change_7d           AS DECIMAL(10,3)) AS route_7d_snow_depth_change_m,
    CAST(avg_route_sunshine_hours       AS DECIMAL(10,2)) AS route_sunshine_hours,
    CAST(avg_3day_gouter_temp_mean_c    AS DECIMAL(10,2)) AS gouter_3d_temp_mean_c,
    CAST(avg_30day_gouter_temp_max_c    AS DECIMAL(10,2)) AS gouter_30d_temp_max_c,

    -- Wind score and sub-scores
    CAST(wind_score                     AS INT)            AS wind_score,
    CAST(summit_wind_score              AS INT)            AS wind_score_summit,
    CAST(gouter_wind_score              AS INT)            AS wind_score_gouter,
    CAST(vallot_wind_score              AS INT)            AS wind_score_vallot,

    -- Temperature score and sub-scores
    CAST(temperature_score              AS INT)            AS temperature_score,
    CAST(summit_cold_score              AS INT)            AS temperature_score_cold,
    CAST(summit_heat_score              AS INT)            AS temperature_score_heat,

    -- Visibility score
    CAST(visibility_score               AS INT)            AS visibility_score,

    -- Grand couloir score and sub-scores
    CAST(grand_couloir_score            AS INT)            AS grand_couloir_score,
    CAST(gouter_freeze_score            AS INT)            AS grand_couloir_score_freeze,
    CAST(thaw_score                     AS INT)            AS grand_couloir_score_thaw,

    -- Avalanche score and sub-scores
    CAST(avalanche_score                AS INT)            AS avalanche_score,
    CAST(thermal_risk                   AS INT)            AS avalanche_risk_thermal,
    CAST(solar_risk                     AS INT)            AS avalanche_risk_solar,
    CAST(accumulation_instability       AS INT)            AS avalanche_risk_dry_slab,
    CAST(degradation_instability        AS INT)            AS avalanche_risk_wet_slab,
    CAST(wind_slab_instability          AS INT)            AS avalanche_risk_wind_slab,

    -- Overall
    CAST(ROUND(
        0.25 * wind_score          +
        0.15 * temperature_score   +
        0.15 * visibility_score    +
        0.25 * grand_couloir_score +
        0.20 * avalanche_score
    )                                   AS INT)            AS overall_score,
    is_viable,
    is_recommended,
    is_recommended
        AND wind_score          >= 75
        AND temperature_score   >= 75
        AND visibility_score    >= 75
        AND grand_couloir_score >= 75
        AND avalanche_score     >= 75                      AS is_ideal
FROM category_scores
{% if is_incremental() %}
-- Look back 30 days to ensure rolling windows have sufficient data for boundary rows
WHERE date > (SELECT MAX(date) FROM {{ this }}) - INTERVAL 30 DAYS
{% endif %}
