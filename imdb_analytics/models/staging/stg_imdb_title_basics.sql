{{ config(materialized='view') }}

select
    tconst,
    primaryTitle,
    originalTitle,
    try_to_number(startYear)      as start_year,
    try_to_number(runtimeMinutes) as runtime_minutes,
    genres
from {{ source('raw', 'IMDB_TITLE_BASICS') }}
