from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os

# Get Snowflake connection
conn = BaseHook.get_connection("snowflake_conn")

default_args = {
    "owner": "data226-group",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Merge existing environment with DBT_* vars
dbt_env = os.environ.copy()
dbt_env.update({
    "DBT_USER": conn.login,
    "DBT_PASSWORD": conn.password,
    "DBT_ACCOUNT": conn.extra_dejson.get("account"),
    "DBT_DATABASE": "DATA226_GROUP_PROJECT",
    "DBT_ROLE": conn.extra_dejson.get("role"),
    "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
    "DBT_TYPE": "snowflake",
})

DBT_DIR = "/opt/airflow/dbt/imdb_analytics"

with DAG(
    dag_id="dbt_imdb_elt",
    default_args=default_args,
    description="Run dbt ELT models for IMDb project",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 3 * * *",
    catchup=False,
    tags=["dbt", "elt", "imdb"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir /opt/airflow/dbt",
        env=dbt_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir /opt/airflow/dbt",
        env=dbt_env,
    )

    dbt_run >> dbt_test
