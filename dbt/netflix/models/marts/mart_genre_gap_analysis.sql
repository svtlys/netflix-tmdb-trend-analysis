with netflix as (

    select
        genre_name,
        count(*) as netflix_count
    from {{ ref('fact_netflix_catalog') }}
    group by genre_name

),

netflix_totals as (

    select sum(netflix_count) as total_netflix_count
    from netflix

),

tmdb as (

    select
        genre_name,
        count(*) as trending_count
    from {{ ref('fact_tmdb_trending_snapshot') }}
    group by genre_name

),

tmdb_totals as (

    select sum(trending_count) as total_trending_count
    from tmdb

),

joined as (

    select
        coalesce(t.genre_name, n.genre_name) as genre_name,
        coalesce(n.netflix_count, 0) as netflix_count,
        coalesce(t.trending_count, 0) as trending_count
    from tmdb t
    full outer join netflix n
        on t.genre_name = n.genre_name

)

select
    genre_name,
    netflix_count,
    trending_count,
    netflix_count / nullif(nt.total_netflix_count, 0) as netflix_share,
    trending_count / nullif(tt.total_trending_count, 0) as trending_share,
    (trending_count / nullif(tt.total_trending_count, 0))
      - (netflix_count / nullif(nt.total_netflix_count, 0)) as share_gap
from joined
cross join netflix_totals nt
cross join tmdb_totals tt
order by share_gap desc