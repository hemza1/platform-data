from __future__ import annotations

from datetime import datetime

from airflow.datasets import Dataset
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_platform"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
DBT_CMD = (
    "DBT_TARGET_PATH=/tmp/dbt_target/snowflake"
    " dbt --no-use-colors"
    " --log-path /tmp/dbt_logs/snowflake"
)
DBT_FLAGS = (
    f"--profiles-dir {DBT_PROFILES_DIR}"
    f" --project-dir {DBT_PROJECT_DIR}"
    f" --target snowflake"
    f" --vars '{{\"source_database\": \"PLATFORM_DB\", \"source_schema\": \"BRONZE\"}}'"
)
SILVER_READY = Dataset("dbt://platform/snowflake/silver/ready")
GOLD_READY   = Dataset("dbt://platform/snowflake/gold/ready")


@dag(
    dag_id="dbt_platform_snowflake",
    description="Run dbt silver and gold models on Snowflake",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "snowflake", "silver", "gold"],
)
def dbt_platform_snowflake_dag():

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps {DBT_FLAGS}",
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"{DBT_CMD} run --select silver {DBT_FLAGS}",
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"{DBT_CMD} run --select gold {DBT_FLAGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --select silver gold {DBT_FLAGS}",
    )

    mark_silver_ready = EmptyOperator(
        task_id="mark_silver_ready",
        outlets=[SILVER_READY],
    )

    mark_gold_ready = EmptyOperator(
        task_id="mark_gold_ready",
        outlets=[GOLD_READY],
    )

    dbt_deps >> dbt_run_silver >> dbt_run_gold >> dbt_test >> [mark_silver_ready, mark_gold_ready]


dbt_platform_snowflake = dbt_platform_snowflake_dag()
