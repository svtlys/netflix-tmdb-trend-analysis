with base as (

    select
        cast(snapshot_timestamp as date) as snapshot_date,
        tmdb_id,
        title,
        genre_name,
        popularity,
        vote_average
    from {{ ref('fact_tmdb_trending_snapshot') }}

),

daily as (

    select
        snapshot_date,
        count(distinct tmdb_id) as trending_title_count,
        avg(vote_average) as avg_vote_average,
        avg(popularity) as avg_popularity
    from base
    group by snapshot_date

),

top_genres as (

    select
        snapshot_date,
        genre_name,
        count(*) as genre_count,
        row_number() over (
            partition by snapshot_date
            order by count(*) desc
        ) as genre_rank
    from base
    group by snapshot_date, genre_name

)

select
    d.snapshot_date,
    d.trending_title_count,
    d.avg_vote_average,
    d.avg_popularity,
    tg.genre_name as top_genre,
    tg.genre_count as top_genre_count
from daily d
left join top_genres tg
    on d.snapshot_date = tg.snapshot_date
   and tg.genre_rank = 1
order by d.snapshot_date desc