from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data226-group",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

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
        bash_command=f"cd {DBT_DIR} && dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test"
    )

    dbt_run >> dbt_test
