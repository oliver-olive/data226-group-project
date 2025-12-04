{{ config(materialized='table') }}

select
    start_year,
    genre,
    count(*)              as num_movies,
    avg(average_rating)   as avg_rating,
    sum(num_votes)        as total_votes
from {{ ref('genre_exploded') }}
where average_rating is not null
group by start_year, genre
order by start_year, genre
