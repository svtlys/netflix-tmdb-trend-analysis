select
    tmdb_id,
    title,

    case
        when genre_name = 'Science Fiction' then 'Sci-Fi'
        else genre_name
    end as genre_name,

    popularity,
    vote_average,
    snapshot_timestamp

from {{ ref('int_tmdb_trending_genres') }}