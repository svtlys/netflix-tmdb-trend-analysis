with source as (

    select *
    from USER_DB_GECKO.RAW.TMDB_TRENDING

),

cleaned as (

    select
        try_cast(id as int) as tmdb_id,

        nullif(trim(title), '') as title,

        try_to_date(release_date) as release_date,

        try_cast(popularity as float) as popularity,

        try_cast(vote_average as float) as vote_average,

        try_cast(vote_count as int) as vote_count,

        genre_ids,

        nullif(trim(original_language), '') as original_language,

        snapshot_timestamp

    from source

)

select *
from cleaned
where tmdb_id is not null
  and title is not null