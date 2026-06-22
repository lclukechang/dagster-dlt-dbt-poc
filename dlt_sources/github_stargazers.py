import os
from typing import Any

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

GITHUB_API = "https://api.github.com"

REPOS = [
    {"slug": "dbt-labs/dbt-core", "name": "dbt_core"},
    {"slug": "apache/airflow", "name": "airflow"},
    {"slug": "dagster-io/dagster", "name": "dagster"},
    {"slug": "duckdb/duckdb", "name": "duckdb"},
    {"slug": "dlt-hub/dlt", "name": "dlt"},
]


def _require_token() -> str:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN environment variable is required")
    return token


def _flatten_stargazer(row: dict[str, Any]) -> dict[str, Any]:
    user = row.get("user") or {}
    return {
        "user_id": user.get("id"),
        "user_login": user.get("login"),
        "starred_at": row.get("starred_at"),
    }


@dlt.source(name="github_stargazers")
def github_stargazers_source(github_token: str = dlt.secrets.value) -> Any:
    token = github_token or _require_token()

    stargazer_resources = [
        {
            "name": f"stargazers__{repo['name']}",
            "endpoint": {"path": f"repos/{repo['slug']}/stargazers"},
        }
        for repo in REPOS
    ]

    config: RESTAPIConfig = {
        "client": {
            "base_url": f"{GITHUB_API}/",
            "headers": {
                "Accept": "application/vnd.github.star+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            "auth": {"type": "bearer", "token": token},
            "paginator": "header_link",
        },
        "resource_defaults": {
            "write_disposition": "merge",
            "primary_key": "user_id",
            "endpoint": {"params": {"per_page": 100}},
        },
        "resources": stargazer_resources,
    }

    api_source = rest_api_source(config, parallelized=True)
    for resource in api_source.resources.values():
        resource.add_map(_flatten_stargazer)

    return api_source
