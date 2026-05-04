select
    show_id,
    title,
    genre_name,
    release_year,
    type
from {{ ref('int_genre_netflix') }}