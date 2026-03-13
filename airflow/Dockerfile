FROM apache/airflow:3.1.7

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-core==1.9.1 \
    dbt-postgres==1.9.1 \
    protobuf>=5.28,<6 \
    apache-airflow-providers-snowflake \
    snowflake-connector-python
