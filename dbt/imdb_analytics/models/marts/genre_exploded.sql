{{ config(materialized='table') }}

with exploded as (
    select
        tconst,
        primaryTitle,
        originalTitle,
        start_year,
        runtime_minutes,
        average_rating,
        num_votes,
        trim(value::string) as genre
    from {{ ref('fct_movies') }},
         lateral flatten(input => split(genres, ','))  -- split and flatten
)

select * from exploded
