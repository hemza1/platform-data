"""
dbt_platform_dag.py

Orchestrates dbt transformations for the platform warehouse.
Runs after source DAGs (dvf_2025, open_meteo) have loaded data into bronze.

Pipeline:
  bronze (Airflow loads) --> silver (dbt) --> gold (dbt) --> [Dataset markers]

Datasets emitted after successful run:
  - dbt://platform/silver/ready
  - dbt://platform/gold/ready
Any downstream DAG (BI refresh, exports, etc.) can subscribe to these.
"""
from __future__ import annotations

from datetime import datetime

from airflow.datasets import Dataset
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_platform"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

DBT_CMD = "dbt --no-use-colors"
DBT_FLAGS = (
    f"--profiles-dir {DBT_PROFILES_DIR}"
    f" --project-dir {DBT_PROJECT_DIR}"
)

# Datasets emitted by this DAG — downstream DAGs can use these as schedule triggers
SILVER_READY = Dataset("dbt://platform/silver/ready")
GOLD_READY   = Dataset("dbt://platform/gold/ready")


@dag(
    dag_id="dbt_platform",
    description="Run dbt silver and gold models after source data is loaded to bronze",
    schedule=None,  # triggered by upstream DAGs (dvf_2025, open_meteo)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "transform", "silver", "gold"],
)
def dbt_platform_dag():

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

    # Marker tasks — emit Dataset events so downstream DAGs can react
    mark_silver_ready = EmptyOperator(
        task_id="mark_silver_ready",
        outlets=[SILVER_READY],
    )

    mark_gold_ready = EmptyOperator(
        task_id="mark_gold_ready",
        outlets=[GOLD_READY],
    )

    dbt_deps >> dbt_run_silver >> dbt_run_gold >> dbt_test >> [mark_silver_ready, mark_gold_ready]


dbt_platform = dbt_platform_dag()
