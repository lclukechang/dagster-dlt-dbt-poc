select
    user_id,
    count(distinct repo) as repos_starred,
    string_agg(distinct repo, ', ' order by repo) as repos
from {{ ref('stg_stargazers') }}
group by user_id
