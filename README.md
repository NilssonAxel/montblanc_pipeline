# montblanc_pipeline

A Databricks data pipeline that fetches historical weather data for Mont Blanc waypoints from the Open-Meteo API, transforms it through a medallion architecture (bronze → silver → gold), and models it with dbt for summit condition analysis.

## Architecture

```
Open-Meteo API
      │
      ▼
  Bronze layer      Raw JSON responses per waypoint per month (Delta)
      │
      ▼
  Silver layer      Flattened, cleaned, deduplicated weather records (Delta)
      │
      ▼
  Gold layer (dbt)  Analytical models for summit planning
```

**Gold models:**
- `daily_conditions` — per-waypoint daily weather summary
- `monthly_conditions` — monthly aggregates per waypoint
- `elevation_gradient` — rate of change in conditions per 100m elevation gain
- `summit_window` — daily viability and scoring for summit attempts
- `seasonal_summary` — cross-year monthly statistics at the summit

**Waypoints:** Chamonix (1035m) → Tête Rousse (3167m) → Goûter (3835m) → Vallot (4362m) → Summit (4808m)

## Prerequisites

| Tool | Purpose | Install |
|---|---|---|
| [uv](https://docs.astral.sh/uv) | Python package manager | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) | Bundle deployment | `brew install databricks` |
| [Terraform](https://developer.hashicorp.com/terraform) | Infrastructure provisioning | `brew install terraform` |

## Setup

**1. Clone and install dependencies**
```bash
git clone <repo-url>
cd montblanc_pipeline
uv sync --dev
```

**2. Authenticate with Databricks**
```bash
databricks configure
```

**3. Provision infrastructure with Terraform**
```bash
cd infrastructure
cp terraform.tfvars.example terraform.tfvars
```
Fill in `terraform.tfvars`:
- `databricks_host` — your workspace URL, e.g. `https://dbc-xxxxxx.cloud.databricks.com`
- `databricks_token` — a personal access token from **Settings → Developer settings → Access tokens**

```bash
terraform init
terraform apply
```

> Terraform is run manually rather than in CI because remote state storage (e.g. S3 or Terraform Cloud) is required to share state between CI runs. Infrastructure changes are infrequent so the manual step is acceptable.

**4. Configure the bundle**

The `warehouse_id` in `databricks.yml` is already set. If you need to change it, find the warehouse ID on the **Overview** tab of your SQL warehouse in Databricks.

**5. Configure dbt**
```bash
cd montblanc_dbt
cp profiles.yml.example ~/.dbt/profiles.yml
```
Fill in `~/.dbt/profiles.yml` for each target:
- `host` — your workspace URL without `https://`, e.g. `dbc-xxxxxx.cloud.databricks.com`
- `http_path` — found under **SQL Warehouses → your warehouse → Connection details**
- `token` — same personal access token as above

```bash
dbt deps
```

**6. Enable Lakehouse Monitoring**

After the pipeline has run at least once and tables are populated, enable monitoring:
```bash
cd infrastructure
```
Add `create_monitors = true` to `terraform.tfvars`, then:
```bash
terraform apply
```
This creates a daily monitoring job on `silver.weather` that tracks data quality and drift. Results are written to the `meta` schema in each environment.

**7. Manual configuration in Databricks UI**

These steps cannot be automated without cloud infrastructure and are set once:
- **Budget alert** — **Settings → Budget policies** — set a spending threshold and email notification
- **SQL warehouse auto-stop** — **SQL Warehouses → your warehouse → Edit** — verify auto-stop is enabled to prevent idle billing

## Security

Credentials are stored as GitHub Actions secrets on the `test` environment (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`). A dedicated service account with minimal permissions is recommended if adding collaborators.

Secrets management tooling (e.g. HashiCorp Vault) is not used here as it requires a persistent server. GitHub secrets is sufficient for a single-developer project.

## Development workflow

All changes must go through a pull request — direct pushes to `main` are not allowed.

```bash
# 1. Create a feature branch
git checkout -b my-feature

# 2. Make changes, then run tests and lint locally
uv run pytest
uv run ruff check .

# 3. Push and open a pull request
git push origin my-feature
# Open a PR on GitHub targeting main
```

CI runs automatically on the PR. Both lint and tests must pass before merging.

**Deploy to dev for manual testing**
```bash
databricks bundle deploy --target dev
databricks bundle run montblanc_pipeline_dev --target dev
```

## CI/CD

On every pull request: lint and unit tests must pass before merging.

On every merge to `main`:
1. Wheel is built once
2. Deployed to `test` automatically
3. Integration tests run against `test`
4. Manual approval required for `prod`
5. Deployed to `prod`
6. Smoke tests run against `prod`

Requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets on the `test` and `prod` GitHub environments.

Serverless compute terminates automatically after each task completes. Verify auto-stop is enabled on the SQL warehouse in the Databricks UI to prevent idle billing between dbt runs.

## Pipeline job

The pipeline runs on a daily schedule in `test` and `prod` with a 2 hour timeout:

`bronze → silver → dbt run → dbt test → dbt source freshness`

Data must be no older than 14 days — a warning is raised at 14 days and the job fails at 16 days. Schedules are paused in `dev`.
