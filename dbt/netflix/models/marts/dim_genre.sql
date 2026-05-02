with netflix_genres as (

    select distinct
        genre_name
    from {{ ref('fact_netflix_catalog') }}

),

tmdb_genres as (

    select distinct
        genre_name
    from {{ ref('fact_tmdb_trending_snapshot') }}

)

select distinct
    coalesce(n.genre_name, t.genre_name) as genre_name
from netflix_genres n
full outer join tmdb_genres t
    on n.genre_name = t.genre_name
where coalesce(n.genre_name, t.genre_name) is not null