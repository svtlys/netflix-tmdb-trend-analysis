select
    show_id,
    title,
    date_added,
    case
    when genre_name = 'Comedies' then 'Comedy'
    when genre_name = 'Dramas' then 'Drama'
    when genre_name = 'Thrillers' then 'Thriller'
    when genre_name = 'Action & Adventure' then 'Action'
    when genre_name = 'Sci-Fi & Fantasy' then 'Sci-Fi'
    when genre_name = 'Romantic Movies' then 'Romance'
    when genre_name = 'Horror Movies' then 'Horror'
    when genre_name = 'Children & Family Movies' then 'Family'
    when genre_name = 'Anime Features' then 'Animation'
    when genre_name = 'Documentaries' then 'Documentary'
    when genre_name = 'Music & Musicals' then 'Music'
    else genre_name
    end as genre_name,

    release_year,
    type

from {{ ref('int_genre_netflix') }}