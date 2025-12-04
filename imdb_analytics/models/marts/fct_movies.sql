{{ config(materialized='table') }}

with basics as (
    select
        tconst,
        primaryTitle,
        originalTitle,
        start_year,
        runtime_minutes,
        genres
    from {{ ref('stg_imdb_title_basics') }}
),

ratings as (
    select
        tconst,
        average_rating,
        num_votes
    from {{ ref('stg_imdb_title_ratings') }}
)

select
    b.tconst,
    b.primaryTitle,
    b.originalTitle,
    b.start_year,
    b.runtime_minutes,
    b.genres,
    r.average_rating,
    r.num_votes
from basics b
left join ratings r
    on b.tconst = r.tconst
