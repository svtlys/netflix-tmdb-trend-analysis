select distinct
    show_id,
    title,
    type,
    release_year,
    rating,
    duration,
    country
from {{ ref('stg_netflix_titles') }}