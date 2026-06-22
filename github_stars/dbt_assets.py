from collections.abc import Mapping
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from github_stars.resources import dbt_project


class GithubStarsDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return "dbt_transform"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=GithubStarsDbtTranslator(),
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
