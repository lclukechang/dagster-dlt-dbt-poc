with unioned as (
    select
        'dbt-labs/dbt-core' as repo,
        user_id,
        user_login,
        cast(starred_at as timestamp) as starred_at
    from {{ source('raw', 'stargazers__dbt_core') }}

    union all

    select
        'apache/airflow' as repo,
        user_id,
        user_login,
        cast(starred_at as timestamp) as starred_at
    from {{ source('raw', 'stargazers__airflow') }}

    union all

    select
        'dagster-io/dagster' as repo,
        user_id,
        user_login,
        cast(starred_at as timestamp) as starred_at
    from {{ source('raw', 'stargazers__dagster') }}

    union all

    select
        'duckdb/duckdb' as repo,
        user_id,
        user_login,
        cast(starred_at as timestamp) as starred_at
    from {{ source('raw', 'stargazers__duckdb') }}

    union all

    select
        'dlt-hub/dlt' as repo,
        user_id,
        user_login,
        cast(starred_at as timestamp) as starred_at
    from {{ source('raw', 'stargazers__dlt') }}
)

select
    {{ dbt.generate_surrogate_key(['user_id', 'starred_at', 'repo']) }} as stargazer_sk,
    repo,
    user_id,
    user_login,
    starred_at
from unioned
