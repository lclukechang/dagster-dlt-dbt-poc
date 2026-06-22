# GitHub Stargazers ELT POC

A local proof-of-concept using **dlt**, **dbt Core**, **Dagster**, and **DuckDB** to extract GitHub stargazer records daily for five public repos and transform them.

## Repos tracked

- [dbt-labs/dbt-core](https://github.com/dbt-labs/dbt-core)
- [apache/airflow](https://github.com/apache/airflow)
- [dagster-io/dagster](https://github.com/dagster-io/dagster)
- [duckdb/duckdb](https://github.com/duckdb/duckdb)
- [dlt-hub/dlt](https://github.com/dlt-hub/dlt)


## Project layout

```
dlt_sources/             # dlt extract logic (no Dagster imports)
github_stars/          # Dagster code location (dlt + dbt assets)
dbt_project/           # dbt models (staging + marts)
data/warehouse.duckdb  # local DuckDB warehouse (gitignored)
```

## Architecture

```
GitHub API  →  dlt (raw schema in DuckDB)  →  dbt (staging + marts)  →  Dagster (daily schedule)
```

**Raw tables** (one per repo): `raw.stargazers__*`
**dbt models**: `analytics_dev_staging.stg_stargazers`, `analytics_dev_marts.mart_user_repos_starred`

## Data transformations

Light overview of each layer. dlt loads **source** data; dbt builds **staging** then **marts**.

### Source (`raw` schema) — dlt

One table per tracked repo, written by dlt from `GET /repos/{owner}/{repo}/stargazers`:

| Table | Repo |
|-------|------|
| `raw.stargazers__dbt_core` | dbt-labs/dbt-core |
| `raw.stargazers__airflow` | apache/airflow |
| `raw.stargazers__dagster` | dagster-io/dagster |
| `raw.stargazers__duckdb` | duckdb/duckdb |
| `raw.stargazers__dlt` | dlt-hub/dlt |

Columns: `user_id`, `user_login`, `starred_at`. Loaded with `merge` on `user_id` so daily re-runs are idempotent.

### Staging — `stg_stargazers`

Unions the five raw tables into one row per star event:

- Adds a `repo` label (`owner/repo` slug)
- Casts `starred_at` to timestamp
- Adds `stargazer_sk` — surrogate key on `(user_id, starred_at, repo)`

### Marts — `mart_user_repos_starred`

Aggregates staging to one row per GitHub user among the five tracked repos:

- `repos_starred` — how many of the five repos that user starred
- `repos` — comma-separated list of which repos

Useful for finding users who star multiple projects in this set (values 1–5).

### Dagster asset groups

| Group | Assets |
|-------|--------|
| `ingest` | 5 dlt stargazer resources |
| `dbt_transform` | `stg_stargazers`, `mart_user_repos_starred` |

## dbt schema YAML

Each dbt model has a colocated `.yml` file declaring the model, column descriptions, and data tests. These run as part of `dbt build` when Dagster materializes the dbt assets.

### Sources — [`dbt_project/models/staging/_sources.yml`](dbt_project/models/staging/_sources.yml)

Declares the five `raw` tables produced by dlt. Each table entry includes:

- **`description`** — what repo the table holds
- **`meta.dagster.asset_key`** — links the dbt source to its upstream dlt asset for Dagster lineage

### Staging — [`dbt_project/models/staging/stg_stargazers.yml`](dbt_project/models/staging/stg_stargazers.yml)

| Column | Description | Tests |
|--------|-------------|-------|
| `stargazer_sk` | Surrogate key on `(user_id, starred_at, repo)` | `not_null`, `unique` |
| `repo` | GitHub repo slug | `not_null` |
| `user_id` | GitHub user ID | `not_null` |
| `user_login` | GitHub username | — |
| `starred_at` | When the star was created | `not_null` |

### Marts — [`dbt_project/models/marts/mart_user_repos_starred.yml`](dbt_project/models/marts/mart_user_repos_starred.yml)

| Column | Description | Tests |
|--------|-------------|-------|
| `user_id` | GitHub user ID | `not_null`, `unique` |
| `repos_starred` | Number of tracked repos starred (1–5) | `not_null` |
| `repos` | Comma-separated repo slugs | — |

To view compiled docs locally: `cd dbt_project && uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .`

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- A GitHub personal access token (no special scopes needed for public repos)

## Setup

```bash
cp .env.example .env
# Edit .env and set GITHUB_TOKEN=ghp_...

uv sync
```

## Run locally

Start the Dagster UI:

```bash
uv run dagster dev
```

Open http://localhost:3000, then materialize all assets (or run the `daily_refresh` job). The schedule runs daily at 06:00 UTC.

## Query the warehouse

The `duckdb` pip package is a library only — it does not install a `duckdb` shell binary. Use Python (included via `uv`) or install the [DuckDB CLI](https://duckdb.org/docs/stable/clients/cli/overview) separately (`brew install duckdb`).

**Python (no extra install):**

```bash
uv run python -c "
import duckdb
con = duckdb.connect('data/warehouse.duckdb')
con.sql('SHOW ALL TABLES').show()
"
```

Interactive session:

```bash
uv run python
```

```python
import duckdb
con = duckdb.connect("data/warehouse.duckdb")
con.sql("SELECT * FROM analytics_dev_marts.mart_user_repos_starred WHERE repos_starred > 1 LIMIT 10").show()
```

**DuckDB CLI (optional, if installed via Homebrew):**

```bash
duckdb data/warehouse.duckdb
```

Example queries (dbt builds into `analytics_dev_staging` / `analytics_dev_marts`):

```sql
-- Staging: one row per star
SELECT * FROM analytics_dev_staging.stg_stargazers LIMIT 10;

-- Users who starred more than one tracked repo
SELECT * FROM analytics_dev_marts.mart_user_repos_starred
WHERE repos_starred > 1
ORDER BY repos_starred DESC
LIMIT 20;
```

## GitHub API limits

- **Rate limit**: 5,000 requests/hour with a token (required).
- **Stargazers pagination cap**: GitHub hard-stops the stargazers endpoint at **400 pages × 100 = 40,000 records**. Repos with more than 40k stars will only return the oldest 40k chronologically.

