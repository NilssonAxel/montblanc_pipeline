terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_schema" "bronze" {
  for_each     = toset(var.environments)
  catalog_name = "montblanc_${each.key}"
  name         = "bronze"
}

resource "databricks_schema" "silver" {
  for_each     = toset(var.environments)
  catalog_name = "montblanc_${each.key}"
  name         = "silver"
}

resource "databricks_schema" "gold" {
  for_each     = toset(var.environments)
  catalog_name = "montblanc_${each.key}"
  name         = "gold"
}

resource "databricks_schema" "meta" {
  for_each     = toset(var.environments)
  catalog_name = "montblanc_${each.key}"
  name         = "meta"
}

# Lakehouse Monitoring — tracks data quality and drift on the silver weather table.
# Requires tables to exist before applying. Set create_monitors = true in
# terraform.tfvars after the first pipeline run.
resource "databricks_quality_monitor" "silver_weather" {
  for_each = var.create_monitors ? toset(var.environments) : toset([])

  table_name         = "montblanc_${each.key}.silver.weather"
  assets_dir         = "/Shared/montblanc_pipeline/${each.key}/monitoring"
  output_schema_name = "montblanc_${each.key}.meta"

  time_series {
    timestamp_col = "date"
    granularities = ["1 day", "1 month"]
  }

  schedule {
    quartz_cron_expression = "0 0 8 * * ?"
    timezone_id            = "Europe/Paris"
  }
}