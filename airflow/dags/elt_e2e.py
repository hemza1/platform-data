"""
elt_e2e.py

DAG unique E2E : Extraction → Chargement Bronze → dbt (silver → gold → tests).

Dépendances :
  [extract_meteo, extract_dvf] >> [load_meteo_bronze, load_dvf_bronze] >> run_dbt >> dbt_test
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Fonctions d'extraction (module-level dans les DAGs existants)
from open_meteo import fetch_meteo, load_meteo_to_bronze
from dvf_2025 import fetch_dvf, load_dvf_to_bronze

DBT_PROJECT_DIR  = "/opt/airflow/dbt_platform"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"


@dag(
    dag_id="elt_e2e",
    description="Pipeline E2E : Extract → Load bronze → dbt silver/gold → tests",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["e2e", "elt", "seance5"],
)
def elt_e2e_dag():

    extract_meteo = PythonOperator(
        task_id="extract_meteo",
        python_callable=fetch_meteo,
    )

    extract_dvf = PythonOperator(
        task_id="extract_dvf",
        python_callable=fetch_dvf,
    )

    load_meteo = PythonOperator(
        task_id="load_meteo_bronze",
        python_callable=load_meteo_to_bronze,
    )

    load_dvf = PythonOperator(
        task_id="load_dvf_bronze",
        python_callable=load_dvf_to_bronze,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt --no-use-colors run {DBT_FLAGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt --no-use-colors test {DBT_FLAGS}",
    )

    [extract_meteo, extract_dvf] >> [load_meteo, load_dvf] >> run_dbt >> dbt_test


elt_e2e = elt_e2e_dag()
