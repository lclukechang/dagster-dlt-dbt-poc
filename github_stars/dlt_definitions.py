from pathlib import Path

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dotenv import load_dotenv
from dlt import pipeline

from dlt_sources.github_stargazers import github_stargazers_source

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"


@dlt_assets(
    dlt_source=github_stargazers_source(),
    dlt_pipeline=pipeline(
        pipeline_name="github_stars",
        dataset_name="raw",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        progress="log",
    ),
    name="stargazers",
    group_name="ingest",
)
def stargazers_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
