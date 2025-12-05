import numpy as np
import pandas as pd
from datetime import datetime

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

SNOWFLAKE_DB = Variable.get("SNOWFLAKE_DB")


def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


def fetch_imdb_training_data():
    conn = get_snowflake_connection()
    query = f"""
        SELECT
            TCONST,
            PRIMARYTITLE,
            ORIGINALTITLE,
            START_YEAR,
            RUNTIME_MINUTES,
            GENRES,
            AVERAGE_RATING,
            NUM_VOTES
        FROM {SNOWFLAKE_DB}.ANALYTICS.FCT_MOVIES
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df = df.rename(
        columns={
            "TCONST": "tconst",
            "PRIMARYTITLE": "primaryTitle",
            "ORIGINALTITLE": "originalTitle",
            "START_YEAR": "startYear",
            "RUNTIME_MINUTES": "runtimeMinutes",
            "GENRES": "genres",
            "AVERAGE_RATING": "averageRating",
            "NUM_VOTES": "numVotes",
        }
    )
    return df

@task
def train_and_evaluate():
    """
    Train on rated movies from 2000–2020,
    evaluate on rated movies from 2021–2024,
    then apply the model to ALL movies from 2020 onward
    (both rated and unrated).

    Returns two dicts (for XCom):
      - predictions: row-level predictions for startYear >= 2020
      - genre_year: genre/year aggregates for startYear >= 2020
    """
    df = fetch_imdb_training_data()

    # ---- Basic cleaning / typing ----
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce")
    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")

    # Need year for everyone, rating may be null (unrated)
    df = df.dropna(subset=["startYear"])

    # Rated vs unrated
    rated_mask = df["averageRating"].notna()
    unrated_mask = df["averageRating"].isna()

    # ---- Era buckets (extend to cover max year in data) ----
    max_year = int(df["startYear"].max()) if not df["startYear"].isna().all() else 2035
    era_bins = [1899, 1949, 1969, 1989, 2004, 2009, 2014, 2019, 2024, max_year + 1]
    era_labels = [
        "1900-1949",
        "1950-1969",
        "1970-1989",
        "1990-2004",
        "2005-2009",
        "2010-2014",
        "2015-2019",
        "2020-2024",
        f"2025-{max_year}",
    ]
    df["era"] = pd.cut(df["startYear"], bins=era_bins, labels=era_labels)

    # ---- Genres one-hot (multi-genre handled here) ----
    df["genres"] = df["genres"].fillna("Unknown")
    genre_dummies = df["genres"].str.get_dummies(sep=",")

    # ---- Era one-hot ----
    era_dummies = pd.get_dummies(df["era"], prefix="era", dummy_na=False)

    # ---- Build feature matrix on ALL rows so columns align ----
    numeric_features = ["startYear", "runtimeMinutes", "numVotes"]
    for col in numeric_features:
        if col not in df.columns:
            df[col] = np.nan

    X_all = pd.concat(
        [
            df[numeric_features],
            genre_dummies,
            era_dummies,
        ],
        axis=1,
    )
    X_all = X_all.fillna(X_all.median(numeric_only=True))

    y_all = df["averageRating"]  # will be NaN for unrated

    # =========================================================
    # 1) TRAIN: rated movies 2000–2020
    # 2) TEST : rated movies 2021–2024
    # =========================================================
    train_mask = rated_mask & (df["startYear"] >= 2000) & (df["startYear"] <= 2020)
    test_mask = rated_mask & (df["startYear"] >= 2021) & (df["startYear"] <= 2024)

    X_train, X_test = X_all[train_mask], X_all[test_mask]
    y_train, y_test = y_all[train_mask], y_all[test_mask]

    print(f"Train size (rated, 2000–2020): {X_train.shape[0]}")
    print(f"Test  size (rated, 2021–2024): {X_test.shape[0]}")

    if X_test.shape[0] == 0:
        print("No rated movies in 2021–2024 for evaluation. Check data.")
        return None

    # ---- Train model ----
    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        n_jobs=1,    # avoid Loky + Airflow multiprocessing warning
        max_depth=None,
    )
    model.fit(X_train, y_train)
    y_pred_test = model.predict(X_test)

    # ---- Eval on 2021–2024 rated movies ----
    mse = mean_squared_error(y_test, y_pred_test)    
    rmse = mse ** 0.5
    mae = mean_absolute_error(y_test, y_pred_test)
    r2 = r2_score(y_test, y_pred_test)

    print("[ML METRICS] train: 2000–2020 rated, test: 2021–2024 rated")
    print(f"  RMSE: {rmse:.3f}")
    print(f"  MAE : {mae:.3f}")
    print(f"  R^2 : {r2:.3f}")

    # =========================================================
    # 3) APPLY MODEL to ALL movies from 2020 onward
    #    (rated & unrated, for visualization / forecasting)
    # =========================================================
    apply_mask = df["startYear"] >= 2020
    X_apply = X_all[apply_mask]
    y_pred_apply = model.predict(X_apply)

    results = df.loc[
        apply_mask,
        ["tconst", "primaryTitle", "startYear", "genres", "numVotes", "averageRating"],
    ].copy()
    results["predictedRating"] = y_pred_apply
    results["hasActualRating"] = results["averageRating"].notna()

    print("Sample predictions for startYear >= 2020:")
    print(
        results[
            [
                "primaryTitle",
                "startYear",
                "genres",
                "averageRating",
                "predictedRating",
                "hasActualRating",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )

    # ---- Genre-year aggregates (2020+ only) ----
    results["genre_list"] = results["genres"].str.split(",")
    results_exploded = results.explode("genre_list")

    genre_year_agg = (
        results_exploded.groupby(["startYear", "genre_list"])
        .agg(
            averageRating=("averageRating", "mean"),
            predictedRating=("predictedRating", "mean"),
            movieCount=("tconst", "count"),
        )
        .reset_index()
        .rename(columns={"startYear": "year", "genre_list": "genre"})
    )

    return {
        "predictions": results.to_dict(orient="records"),
        "genre_year": genre_year_agg.to_dict(orient="records"),
    }

@task
def publish_results(data: dict):
    """
    Take output from train_and_evaluate and write to Snowflake.
    """
    if not data:
        print("No data to publish (train_and_evaluate returned None).")
        return

    preds = pd.DataFrame(data["predictions"])
    genre_year_agg = pd.DataFrame(data["genre_year"])

    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
        cur.execute("USE SCHEMA ANALYTICS")

        # IMDB_RATING_PREDICTIONS
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS IMDB_RATING_PREDICTIONS (
                tconst STRING,
                primaryTitle STRING,
                startYear INTEGER,
                genres STRING,
                numVotes INTEGER,
                averageRating FLOAT,
                predictedRating FLOAT
            );
            """
        )
        cur.execute("TRUNCATE TABLE IMDB_RATING_PREDICTIONS;")
        write_pandas(
            conn,
            preds[
                [
                    "tconst",
                    "primaryTitle",
                    "startYear",
                    "genres",
                    "numVotes",
                    "averageRating",
                    "predictedRating",
                ]
            ],
            table_name="IMDB_RATING_PREDICTIONS",
            database=SNOWFLAKE_DB,
            schema="ANALYTICS",
            quote_identifiers=False,
        )

        # IMDB_GENRE_RATING_TRENDS
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS IMDB_GENRE_RATING_TRENDS (
                year INTEGER,
                genre STRING,
                averageRating FLOAT,
                predictedRating FLOAT,
                movieCount INTEGER
            );
            """
        )
        cur.execute("TRUNCATE TABLE IMDB_GENRE_RATING_TRENDS;")
        write_pandas(
            conn,
            genre_year_agg[["year", "genre", "averageRating", "predictedRating", "movieCount"]],
            table_name="IMDB_GENRE_RATING_TRENDS",
            database=SNOWFLAKE_DB,
            schema="ANALYTICS",
            quote_identifiers=False,
        )

        print("Published predictions + genre-year trends to Snowflake.")
        cur.execute("COMMIT;")
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="movie_genre_rating_ml",
    start_date=datetime(2025, 11, 20),
    schedule_interval="0 4 * * *", 
    catchup=False,
    tags=["ML", "movie"],
) as dag:
    train_out = train_and_evaluate()
    publish_results(train_out)
