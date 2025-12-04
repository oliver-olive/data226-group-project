import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_DB = Variable.get("SNOWFLAKE_DB")
TMDB_BEARER_TOKEN = Variable.get("TMDB_BEARER_TOKEN")

TMDB_NOW_PLAYING_URL = "https://api.themoviedb.org/3/movie/now_playing"


def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


def extract_tmdb_now_playing():
    """
    Fetch ALL pages of TMDb 'now_playing' and return a pandas DataFrame.
    """
    headers = {
        "Authorization": f"Bearer {TMDB_BEARER_TOKEN}",
        "Content-Type": "application/json;charset=utf-8",
    }

    all_results = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        params = {
            "language": "en-US",
            "page": page,
            "region": "US",  # optional, can remove
        }
        print(f"Requesting TMDb now_playing page {page}")
        resp = requests.get(TMDB_NOW_PLAYING_URL, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        total_pages = data.get("total_pages", 1)
        results = data.get("results", [])
        all_results.extend(results)
        page += 1

    if not all_results:
        print("No TMDb now_playing results.")
        return pd.DataFrame()

    df = pd.DataFrame(all_results)
    print(f"TMDb now_playing loaded into DataFrame with shape {df.shape}")
    return df


def transform_tmdb_now_playing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and clean columns for Snowflake.
    """
    if df.empty:
        return df

    df = df.copy()

    # release_date -> DATE
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date

    # convert genre_ids list to comma-separated string
    def _genre_list_to_str(x):
        if isinstance(x, list):
            return ",".join(str(g) for g in x)
        return None

    df["genre_ids_str"] = df["genre_ids"].apply(_genre_list_to_str)

    # numeric / types
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
    df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce").astype("Int64")

    # snapshot_date = when we fetched this
    df["snapshot_date"] = datetime.utcnow().date()

    # final columns
    df = df[
        [
            "id",
            "title",
            "original_title",
            "original_language",
            "release_date",
            "popularity",
            "vote_average",
            "vote_count",
            "genre_ids_str",
            "adult",
            "snapshot_date",
        ]
    ]

    df.rename(columns={"id": "tmdb_id", "genre_ids_str": "genre_ids"}, inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f"Transformed TMDb now_playing shape: {df.shape}")
    return df


def load_tmdb_now_playing_to_snowflake(df: pd.DataFrame):
    """
    Full-refresh load into RAW.TMDB_NOW_PLAYING
    """
    if df.empty:
        print("Empty DataFrame, nothing to load to Snowflake.")
        return

    conn = get_snowflake_connection()
    cur = conn.cursor()
    table_name = "TMDB_NOW_PLAYING"
    try:
        cur.execute("BEGIN;")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                tmdb_id INTEGER NOT NULL,
                title STRING,
                original_title STRING,
                original_language STRING,
                release_date DATE,
                popularity FLOAT,
                vote_average FLOAT,
                vote_count INTEGER,
                genre_ids STRING,
                adult BOOLEAN,
                snapshot_date DATE
            );
            """
        )
        # full refresh each run
        cur.execute(f"DELETE FROM {table_name};")
        print(f"Loading DataFrame into Snowflake table {table_name}...")
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            quote_identifiers=False,
        )

        if success:
            print(f"Loaded {nrows} rows into {table_name} using {nchunks} chunk(s).")
        else:
            print("write_pandas failed to load data.")
        cur.execute("COMMIT;")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print("Error loading TMDB_NOW_PLAYING:", e)
        raise
    finally:
        cur.close()
        conn.close()


@task
def etl_tmdb_now_playing():
    df_raw = extract_tmdb_now_playing()
    df_clean = transform_tmdb_now_playing(df_raw)
    load_tmdb_now_playing_to_snowflake(df_clean)


with DAG(
    dag_id="tmdb_now_playing_etl",
    start_date=datetime(2025, 12, 1),
    schedule_interval="0 3 * * *", 
    catchup=False,
    tags=["ETL", "tmdb", "movie"],
) as dag:
    etl_tmdb_now_playing()

