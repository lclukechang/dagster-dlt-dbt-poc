from dotenv import load_dotenv
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job
from dagster_dlt import DagsterDltResource

from github_stars.dbt_assets import dbt_models
from github_stars.dlt_definitions import stargazers_assets
from github_stars.resources import dbt_resource

load_dotenv()

daily_refresh_job = define_asset_job(
    name="daily_refresh",
    selection=AssetSelection.all(),
)

daily_refresh_schedule = ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[stargazers_assets, dbt_models],
    resources={
        "dlt": DagsterDltResource(),
        "dbt": dbt_resource,
    },
    jobs=[daily_refresh_job],
    schedules=[daily_refresh_schedule],
)
