with source as (

    select *
    from USER_DB_GECKO.RAW.TMDB_GENRES

),

cleaned as (

    select
        try_cast(genre_id as int) as genre_id,
        nullif(trim(genre_name), '') as genre_name
    from source

)

select *
from cleaned
where genre_id is not null
  and genre_name is not null