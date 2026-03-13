import json
from datetime import datetime
from pathlib import Path


import requests


from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

OUT_DIR = Path("/opt/airflow/data/meteo")

LAT = 43.297
LON = 5.3811
API_URL = "https://api.open-meteo.com/v1/forecast"

TRANSFORM_METEO_SQL = """
DROP TABLE IF EXISTS silver.meteo_quotidien;

CREATE TABLE silver.meteo_quotidien AS
SELECT
    date_meteo::date               AS date_meteo,
    COALESCE(weather_code::int, 0) AS weather_code
FROM bronze.meteo_quotidien
;
"""


def load_meteo_to_bronze():
    import pandas as pd
    from sqlalchemy import create_engine
    """Lecture du JSON -> insertion dans bronze.meteo_quotidien."""
    src = OUT_DIR / "marseille_forecast.json"
    if not src.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {src}")

    with open(src, encoding="utf-8") as f:
        data = json.load(f)

    daily = data["daily"]

    df = pd.DataFrame({
        "date_meteo": daily["time"],
        "weather_code": daily["weather_code"],
    })

    conn = BaseHook.get_connection("postgres_warehouse")
    port = conn.port or 5432
    dbname = conn.schema or "warehouse"
    engine = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{port}/{dbname}"
    )

    df.to_sql(
        "meteo_quotidien",
        engine,
        schema="bronze",
        if_exists="append",
        index=False,
    )

    print(f"{len(df)} lignes chargées dans bronze.meteo_quotidien")


def fetch_meteo() -> str:
    """Extraction Open-Meteo — importable par d'autres DAGs."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    params = {
        "latitude": LAT,
        "longitude": LON,
        "daily": "weather_code",
        "hourly": "temperature_2m",
        "timezone": "auto",
    }

    api_url = Variable.get("OPEN_METEO_API_URL", default_var=API_URL)
    r = requests.get(api_url, params=params, timeout=(10, 60))
    r.raise_for_status()
    payload = r.json()

    out_path = OUT_DIR / "marseille_forecast.json"
    tmp_path = out_path.with_suffix(".json.part")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    tmp_path.replace(out_path)
    print(f"Météo sauvegardée: {out_path}")
    return str(out_path)


@dag(
    dag_id="extract_open_meteo_marseille",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["seance2", "extraction", "meteo", "open-meteo"],
)
def open_meteo_dag():
    @task(retries=3)
    def fetch_and_save() -> str:
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        params = {
            "latitude": LAT,
            "longitude": LON,
            "daily": "weather_code",
            "hourly": "temperature_2m",
            "timezone": "auto",
        }

        api_url = Variable.get("OPEN_METEO_API_URL", default_var=API_URL)
        r = requests.get(api_url, params=params, timeout=(10, 60))
        r.raise_for_status()
        payload = r.json()

        out_path = OUT_DIR / "marseille_forecast.json"
        tmp_path = out_path.with_suffix(".json.part")

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        tmp_path.replace(out_path)

        print(f"Météo sauvegardée: {out_path}")
        return str(out_path)

    task_fetch = fetch_and_save()

    load_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_meteo_to_bronze,
    )

    transform_silver = SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="postgres_warehouse",
        sql=TRANSFORM_METEO_SQL,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_platform",
        trigger_dag_id="dbt_platform",
        wait_for_completion=False,
    )

    task_fetch >> load_bronze >> transform_silver >> trigger_dbt


open_meteo = open_meteo_dag()