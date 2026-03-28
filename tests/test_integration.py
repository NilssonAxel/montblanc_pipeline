"""
Integration tests — run after deploying the bundle to the test environment.
Triggers the pipeline job, waits for completion, and asserts all output tables
have been populated in montblanc_test.
"""
import datetime
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState
from pyspark.sql import SparkSession

from montblanc_pipeline.config import CATALOG

JOB_NAME = "montblanc_pipeline_test"
JOB_TIMEOUT = datetime.timedelta(minutes=30)


@pytest.fixture(scope="module")
def workspace() -> WorkspaceClient:
    return WorkspaceClient()


@pytest.fixture(scope="module")
def completed_run(workspace: WorkspaceClient):
    """Trigger the pipeline job and wait for it to complete. Shared across tests."""
    jobs = list(workspace.jobs.list(name=JOB_NAME))
    assert jobs, f"Job '{JOB_NAME}' not found — has the bundle been deployed to test?"

    job = next(j for j in jobs if j.settings.name == JOB_NAME)
    run = workspace.jobs.run_now(job_id=job.job_id).result(timeout=JOB_TIMEOUT)
    return run


def test_pipeline_run_succeeds(completed_run):
    """The pipeline job completes without errors."""
    assert completed_run.state.result_state == RunResultState.SUCCESS, \
        f"Job run failed with state: {completed_run.state.result_state}"


def test_silver_weather_has_rows(spark: SparkSession, completed_run):
    """Silver weather table has been populated."""
    df = spark.table(f"{CATALOG}.silver.weather")
    assert df.count() > 0, "silver.weather is empty"


def test_gold_tables_have_rows(spark: SparkSession, completed_run):
    """All gold models have been populated."""
    for table in [
        "daily_conditions",
        "summit_window",
        "elevation_gradient",
    ]:
        df = spark.table(f"{CATALOG}.gold.{table}")
        assert df.count() > 0, f"gold.{table} is empty"


def test_watermark_has_both_layers(spark: SparkSession, completed_run):
    """Watermark table has entries for both bronze and silver layers."""
    df = spark.table(f"{CATALOG}.meta.watermark")
    layers = {row.layer for row in df.collect()}
    assert "bronze" in layers, "Missing bronze watermark"
    assert "silver" in layers, "Missing silver watermark"


def test_silver_has_all_waypoints(spark: SparkSession, completed_run):
    """All five waypoints are present in the silver table."""
    expected = {"chamonix", "tete_rousse", "gouter", "vallot", "summit"}
    df = spark.table(f"{CATALOG}.silver.weather")
    actual = {row.waypoint_name for row in df.select("waypoint_name").distinct().collect()}
    assert expected == actual, f"Missing waypoints: {expected - actual}"
