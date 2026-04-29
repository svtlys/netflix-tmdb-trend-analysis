with source as (

    select * 
    from raw.netflix_titles

),

cleaned as (

    select
        trim(show_id) as show_id,
        trim(type) as content_type,
        trim(title) as title,

        try_to_date(date_added, 'MMMM DD, YYYY') as date_added,
        release_year::int as release_year,

        trim(rating) as rating,
        duration::int as duration_minutes,

        trim(listed_in) as genre,
        trim(country) as country,

        current_timestamp() as loaded_at

    from source

)

select *
from cleaned