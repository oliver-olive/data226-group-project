{{ config(materialized='table') }}

select
    genre,
    count(*)              as num_movies,
    avg(average_rating)   as avg_rating,
    sum(num_votes)        as total_votes
from {{ ref('genre_exploded') }}
where average_rating is not null
group by genre
order by num_movies desc
