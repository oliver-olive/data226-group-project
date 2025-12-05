{{ config(materialized='view') }}

select
    tconst,
    averageRating as average_rating,
    numVotes      as num_votes
from {{ source('raw', 'IMDB_TITLE_RATINGS') }}
