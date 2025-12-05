FROM apache/airflow:2.10.1

# Airflow image expects pip installs to run as "airflow" user
USER airflow

RUN pip install --no-cache-dir \
    "dbt-core==1.8.7" \
    "dbt-snowflake==1.8.4" \
    scikit-learn \
    apache-airflow-providers-snowflake \
    snowflake-connector-python
