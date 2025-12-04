# data226-group-project
## ETL Overview

We have **two ETL pipelines** (Airflow DAGs) writing into Snowflake:

1. **IMDb ETL (historical movies)** – `movie_etl`
2. **TMDb ETL (current “now playing” movies)** – `tmdb_now_playing_etl`

Snowflake DB name is stored in Airflow Variable: `SNOWFLAKE_DB`  
Snowflake connection: `snowflake_conn`  
TMDb token: `TMDB_BEARER_TOKEN` (Airflow Variable)

---

### 1. IMDb ETL – `movie_etl`

**Source:** IMDb non-commercial datasets  
**Files used:**  
- `title.basics.tsv.gz`  
- `title.ratings.tsv.gz`  

**Tables:**

- `RAW.IMDB_TITLE_BASICS`  
  - Columns: `tconst, primaryTitle, originalTitle, startYear, runtimeMinutes, genres`  
  - Filters:
    - `titleType = 'movie'`
    - `isAdult = 0`
    - `startYear >= 2000`

- `RAW.IMDB_TITLE_RATINGS`  
  - Columns: `tconst, averageRating, numVotes`

**DAG logic:**

- `etl_title_basics`: extract → transform → full refresh load into `IMDB_TITLE_BASICS`
- `etl_title_ratings`: extract → transform → full refresh load into `IMDB_TITLE_RATINGS`


