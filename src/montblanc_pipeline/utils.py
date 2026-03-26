import logging
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, DateType, StringType
from datetime import date
import montblanc_pipeline.config as config
from databricks.sdk.runtime import spark

logger = logging.getLogger(__name__)


def get_watermark(layer: str) -> date:
    try:
        meta_watermark = f"{config.CATALOG}.meta.watermark"
        df = spark.table(meta_watermark)
        result = df.filter(F.col("layer") == layer) \
                   .agg(F.max("last_completed_date")).collect()[0][0]
        return result if result is not None else date(1900, 1, 1)
    except Exception:
        return date(1900, 1, 1)


def update_watermark(layer: str, last_completed_date: date) -> None:
    if last_completed_date is None:
        logger.warning("No watermark to update for %s — skipping.", layer)
        return

    meta_watermark = f"{config.CATALOG}.meta.watermark"
    schema = StructType([
        StructField("layer", StringType(), False),
        StructField("last_completed_date", DateType(), False)
    ])

    row = [(layer, last_completed_date)]
    df = spark.createDataFrame(row, schema)

    if spark.catalog.tableExists(meta_watermark):
        df.write.format("delta") \
          .mode("overwrite") \
          .option("replaceWhere", f"layer = '{layer}'") \
          .saveAsTable(meta_watermark)
    else:
        df.write.format("delta") \
          .mode("overwrite") \
          .saveAsTable(meta_watermark)

    logger.info("Watermark updated for %s: %s", layer, last_completed_date)


def get_max_date(df: DataFrame) -> date | None:
    return df.agg(F.max("date")).collect()[0][0]


def write_delta_table(df: DataFrame, table_name: str, mode: str = "append") -> None:
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    logger.info("Written to %s", table_name)
