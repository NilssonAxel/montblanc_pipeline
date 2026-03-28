import re
import logging
import argparse
import montblanc_pipeline.config as config
from montblanc_pipeline.bronze import load_bronze
from montblanc_pipeline.silver import load_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_CATALOG_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def _parse_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--end-date-active", default=None)
    args = parser.parse_args()

    if not _CATALOG_RE.match(args.catalog):
        raise ValueError(f"Invalid catalog name: {args.catalog!r}")
    config.CATALOG = args.catalog

    if args.start_date is not None:
        config.START_DATE = args.start_date
    if args.end_date is not None:
        config.END_DATE = args.end_date
    if args.end_date_active is not None:
        config.END_DATE_ACTIVE = args.end_date_active.lower() == "true"


def run_bronze():
    _parse_args()
    try:
        load_bronze()
    except Exception:
        logger.exception("Bronze load failed")
        raise


def run_silver():
    _parse_args()
    try:
        load_silver()
    except Exception:
        logger.exception("Silver load failed")
        raise


if __name__ == "__main__":
    run_bronze()
    run_silver()
