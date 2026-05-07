with base as (

    select
        show_id,
        title,
        type,
        release_year,
        rating,
        duration,
        listed_in,
        country,
        date_added
    from {{ ref('stg_netflix_titles') }}

),

flattened as (

    select
        show_id,
        title,
        type,
        release_year,
        rating,
        duration,
        date_added,
        trim(f.value::string) as genre_name,
        country
    from base,
    lateral flatten(input => split(listed_in, ',')) f

)

select *
from flattened
where genre_name is not null
  and genre_name <> ''