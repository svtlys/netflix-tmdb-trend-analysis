with source as (

    select *
    from {{ source('raw', 'netflix_titles') }}

),

cleaned as (

    select
        nullif(trim(show_id), '') as show_id,
        trim(type) as type,
        nullif(trim(title), '') as title,
        try_to_date(nullif(trim(date_added), ''), 'MMMM DD, YYYY') as date_added,
        try_cast(release_year as int) as release_year,
        nullif(trim(rating), '') as rating,
        nullif(trim(duration), '') as duration,
        nullif(trim(listed_in), '') as listed_in,
        nullif(trim(country), '') as country
    from source

),

transformed as (

    select
        show_id,
        type,
        title,
        date_added,
        release_year,
        rating,
        cast(replace(duration, ' min', '') as int) as duration,
        trim(f_genre.value::string) as listed_in,
        trim(f_country.value::string) as country
    from cleaned,
    lateral flatten(input => split(listed_in, ',')) f_genre,
    lateral flatten(input => split(ifnull(country, 'Unknown'), ',')) f_country
    where type = 'Movie'

)

select *
from transformed
where show_id is not null
  and title is not null