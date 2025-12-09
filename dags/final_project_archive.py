import requests
import pandas as pd
from io import BytesIO
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import task
from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

IMDB_BASE_URL = "https://datasets.imdbws.com/"
SNOWFLAKE_DB = Variable.get("SNOWFLAKE_DB")


def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


def extract(dataset_name):
    """
    Download IMDb dataset into a pandas DataFrame.
    """
    file_name = f"{dataset_name}.tsv.gz"
    url = IMDB_BASE_URL + file_name
    print(f"Downloading IMDb dataset from {url}")
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    df = pd.read_csv(
        BytesIO(resp.content),
        sep="\t",
        compression="gzip",
        na_values="\\N",
        dtype=str
    )
    print(f"{file_name} loaded into DataFrame with shape {df.shape}")
    return df


def transform_title_basics(df):
    df = df.copy()
    df.replace("\\N", pd.NA, inplace=True)
    df["isAdult"] = df["isAdult"].astype("Int64")
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce").astype("Int64")
    df["endYear"] = pd.to_numeric(df["endYear"], errors="coerce").astype("Int64")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce").astype("Int64")
    df = df[
        (df["titleType"] == "movie") &
        (df["isAdult"] == 0) &
        (df["startYear"] >= 2000)
        ]
    df["startYear"] = df["startYear"].astype("Int64")
    df = df[
        [
            "tconst",
            "primaryTitle",
            "originalTitle",
            "startYear",
            "runtimeMinutes",
            "genres",
        ]
    ]
    print(f"Transformed title.basics shape: {df.shape}")
    return df


def transform_title_ratings(df):
    df = df.copy()
    df.replace("\\N", pd.NA, inplace=True)

    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce").astype("Int64")

    df = df[["tconst", "averageRating", "numVotes"]]

    df.reset_index(drop=True, inplace=True)
    print(f"Transformed title.ratings shape: {df.shape}")
    return df



def load_title_basics_to_snowflake(df):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    table_name = "IMDB_TITLE_BASICS"
    try:
        cur.execute("BEGIN;")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                tconst STRING NOT NULL,
                primaryTitle STRING,
                originalTitle STRING,
                startYear INTEGER,
                runtimeMinutes INTEGER,
                genres STRING
            );
            """
        )
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
        print("Error loading IMDB_TITLE_BASICS:", e)
        raise
    finally:
        cur.close()
        conn.close()



def load_title_ratings_to_snowflake(df):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    table_name = "IMDB_TITLE_RATINGS"
    try:
        cur.execute("BEGIN;")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA RAW")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                tconst STRING NOT NULL,
                averageRating FLOAT,
                numVotes INTEGER
            );
            """
        )
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
        print("Error loading IMDB_TITLE_RATINGS:", e)
        raise
    finally:
        cur.close()
        conn.close()


@task
def etl_title_basics():
    df_raw = extract("title.basics")
    df_clean = transform_title_basics(df_raw)
    load_title_basics_to_snowflake(df_clean)

@task
def etl_title_ratings():
    df_raw = extract("title.ratings")
    df_clean = transform_title_ratings(df_raw)
    load_title_ratings_to_snowflake(df_clean)

    
with DAG(
        dag_id="movie_etl",
        start_date=datetime(2025, 11, 20),
        schedule_interval="0 1 * * *", 
        catchup=False,
        tags=["ETL", "movie"]
) as dag:
    basics_task = etl_title_basics()
    ratings_task = etl_title_ratings()

    basics_task >> ratings_task

