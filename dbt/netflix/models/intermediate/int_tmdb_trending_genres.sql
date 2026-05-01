with trending as (

    select *
    from {{ ref('stg_tmdb_trending') }}

),

flattened as (

    select
        tmdb_id,
        title,
        release_date,
        popularity,
        vote_average,
        vote_count,
        original_language,
        snapshot_timestamp,
        f.value::int as genre_id
    from trending,
    lateral flatten(input => parse_json(genre_ids)) f

),

genres as (

    select *
    from {{ ref('stg_tmdb_genres') }}

)

select
    f.tmdb_id,
    f.title,
    f.release_date,
    f.popularity,
    f.vote_average,
    f.vote_count,
    f.original_language,
    f.snapshot_timestamp,
    f.genre_id,
    g.genre_name
from flattened f
left join genres g
    on f.genre_id = g.genre_id