select
    tmdb_id,
    title,
    genre_name,
    popularity,
    vote_average,
    snapshot_timestamp
from {{ ref('int_tmdb_trending_genres') }}