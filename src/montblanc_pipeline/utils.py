import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType
from datetime import date
from montblanc_pipeline.config import CATALOG, META_WATERMARK
from databricks.sdk.runtime import spark

logger = logging.getLogger(__name__)


def get_watermark(layer: str) -> date:
    try:
        df = spark.table(META_WATERMARK)
        result = df.filter(F.col("layer") == layer) \
                   .agg(F.max("last_completed_date")).collect()[0][0]
        return result if result is not None else date(1900, 1, 1)
    except Exception:
        return date(1900, 1, 1)


def update_watermark(layer: str, last_completed_date: date) -> None:
    if last_completed_date is None:
        logger.warning("No watermark to update for %s — skipping.", layer)
        return

    schema = StructType([
        StructField("layer", StringType(), False),
        StructField("last_completed_date", DateType(), False)
    ])

    row = [(layer, last_completed_date)]
    df = spark.createDataFrame(row, schema)

    if spark.catalog.tableExists(META_WATERMARK):
        df.write.format("delta") \
          .mode("overwrite") \
          .option("replaceWhere", f"layer = '{layer}'") \
          .saveAsTable(META_WATERMARK)
    else:
        df.write.format("delta") \
          .mode("overwrite") \
          .saveAsTable(META_WATERMARK)

    logger.info("Watermark updated for %s: %s", layer, last_completed_date)


def get_max_date(df: "DataFrame") -> date | None:
    return df.agg(F.max("date")).collect()[0][0]


def write_delta_table(df: "DataFrame", table_name: str, mode: str = "append") -> None:
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    logger.info("Written to %s", table_name)
