import logging
from pyspark.sql import functions as F, DataFrame
from pyspark.sql import Window as W
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType
from databricks.sdk.runtime import spark
from delta.tables import DeltaTable
import montblanc_pipeline.config as config
from montblanc_pipeline.utils import get_watermark, update_watermark, get_max_date

logger = logging.getLogger(__name__)

RESPONSE_SCHEMA = StructType([
    StructField("daily", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("temperature_2m_max", ArrayType(DoubleType()), True),
        StructField("temperature_2m_min", ArrayType(DoubleType()), True),
        StructField("windspeed_10m_max", ArrayType(DoubleType()), True),
        StructField("windgusts_10m_max", ArrayType(DoubleType()), True),
        StructField("precipitation_sum", ArrayType(DoubleType()), True),
        StructField("snowfall_sum", ArrayType(DoubleType()), True),
        StructField("snow_depth_max", ArrayType(DoubleType()), True),
        StructField("surface_pressure_mean", ArrayType(DoubleType()), True),
        StructField("cloudcover_mean", ArrayType(DoubleType()), True),
        StructField("daylight_duration", ArrayType(DoubleType()), True),
        StructField("sunshine_duration", ArrayType(DoubleType()), True)
    ]), True)
])


def read_bronze(spark) -> DataFrame:
    latest = get_watermark("silver")
    return spark.table(f"{config.CATALOG}.bronze.weather_raw").filter(F.col("period_start") > latest)


def transform_silver(df: DataFrame) -> DataFrame:
    # Parse JSON string into struct
    df = df.withColumn("response", F.from_json(F.col("response_json"), RESPONSE_SCHEMA))

    # Zip all daily arrays together
    df = df.withColumn("daily", F.arrays_zip(
        F.col("response.daily.time"),
        F.col("response.daily.temperature_2m_max"),
        F.col("response.daily.temperature_2m_min"),
        F.col("response.daily.windspeed_10m_max"),
        F.col("response.daily.windgusts_10m_max"),
        F.col("response.daily.precipitation_sum"),
        F.col("response.daily.snowfall_sum"),
        F.col("response.daily.snow_depth_max"),
        F.col("response.daily.surface_pressure_mean"),
        F.col("response.daily.cloudcover_mean"),
        F.col("response.daily.daylight_duration"),
        F.col("response.daily.sunshine_duration")
    ))

    # Explode into one row per day
    df = df.withColumn("daily", F.explode(F.col("daily")))

    # Extract fields and rename
    df = df.select(
        F.col("waypoint_name"),
        F.col("elevation").alias("elevation"),
        F.to_date(F.col("daily.time"), "yyyy-MM-dd").alias("date"),
        F.col("daily.temperature_2m_max").cast("decimal(10,2)").alias("temperature_max_c"),
        F.col("daily.temperature_2m_min").cast("decimal(10,2)").alias("temperature_min_c"),
        (F.col("daily.windspeed_10m_max") / F.lit(3.6)).cast("decimal(10,2)").alias("windspeed_max_ms"),  # convert km/h to m/s
        (F.col("daily.windgusts_10m_max") / F.lit(3.6)).cast("decimal(10,2)").alias("windgusts_max_ms"),  # convert km/h to m/s
        F.col("daily.precipitation_sum").cast("decimal(10,2)").alias("precipitation_mm"),
        F.col("daily.snowfall_sum").cast("decimal(10,2)").alias("snowfall_cm"),
        F.col("daily.snow_depth_max").cast("decimal(10,2)").alias("snow_depth_m"),
        F.col("daily.surface_pressure_mean").cast("decimal(10,2)").alias("pressure_hpa"),
        F.col("daily.cloudcover_mean").cast("decimal(10,2)").alias("cloudcover_pct"),
        (F.col("daily.daylight_duration") / F.lit(3600)).cast("decimal(10,2)").alias("daylight_hours"),  # convert seconds to hours
        (F.col("daily.sunshine_duration") / F.lit(3600)).cast("decimal(10,2)").alias("sunshine_hours"),  # convert seconds to hours
        F.col("period_end")
    )

    # Derive air density -- ρ = P / (R × T) where R (specific gas constant for dry air) = 287.05, T (temp) in Kelvin
    df = df.withColumn("air_density_kgm3",
        ((F.col("pressure_hpa") * 100) /  # convert hPa to Pascals (P)
        (F.lit(287.05) * (F.col("temperature_min_c") + F.lit(273.15)))).cast("decimal(10,2)")  # convert C to Kelvin
    )

    # Deduplicate — keep the most recent fetch for each waypoint+date
    window = W.partitionBy("waypoint_name", "date").orderBy(F.col("period_end").desc())
    df = df.withColumn("dupes", F.row_number().over(window))
    df = df.filter(F.col("dupes") == 1).drop("dupes", "period_end")

    return df


def write_silver(df: DataFrame) -> None:
    silver_weather = f"{config.CATALOG}.silver.weather"
    if spark.catalog.tableExists(silver_weather):
        delta_table = DeltaTable.forName(spark, silver_weather)
        delta_table.alias("t").merge(
            df.alias("s"),
            "t.waypoint_name = s.waypoint_name AND t.date = s.date"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        df.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .saveAsTable(silver_weather)

    logger.info("Written to %s", silver_weather)


def load_silver() -> None:
    df = read_bronze(spark)
    df = transform_silver(df)
    write_silver(df)
    update_watermark("silver", get_max_date(df))
    logger.info("Silver load complete.")
