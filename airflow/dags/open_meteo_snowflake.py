import json
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

OUT_DIR = Path("/opt/airflow/data/meteo")
LAT = 43.297
LON = 5.3811
API_URL = "https://api.open-meteo.com/v1/forecast"

TRANSFORM_METEO_SQL = """
CREATE OR REPLACE TABLE PLATFORM_DB.SILVER.meteo_quotidien_gold AS
SELECT
    TRY_TO_DATE(date_meteo)        AS date_meteo,
    COALESCE(weather_code::int, 0) AS weather_code
FROM PLATFORM_DB.BRONZE.meteo_quotidien
;
"""


def load_meteo_to_bronze():
    import pandas as pd
    from sqlalchemy import create_engine
    from airflow.sdk.bases.hook import BaseHook

    src = OUT_DIR / "marseille_forecast.json"
    if not src.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {src}")

    with open(src, encoding="utf-8") as f:
        data = json.load(f)

    daily = data["daily"]
    df = pd.DataFrame({
        "DATE_METEO": daily["time"],
        "WEATHER_CODE": daily["weather_code"],
    })

    conn = BaseHook.get_connection("snowflake_platform")
    extra = conn.extra_dejson
    account = extra.get("account", "")
    warehouse = extra.get("warehouse", "PLATFORM_WH")
    database = extra.get("database", "PLATFORM_DB")
    role = extra.get("role", "ACCOUNTADMIN")

    engine = create_engine(
        f"snowflake://{conn.login}:{quote_plus(conn.password)}@{account}/"
        f"{database}/BRONZE?warehouse={warehouse}&role={role}"
    )

    df.to_sql(
        "METEO_QUOTIDIEN",
        engine,
        schema="BRONZE",
        if_exists="append",
        index=False,
    )
    print(f"{len(df)} lignes chargées dans PLATFORM_DB.BRONZE.METEO_QUOTIDIEN")


@dag(
    dag_id="extract_open_meteo_marseille_snowflake",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "meteo", "bronze"],
)
def open_meteo_snowflake_dag():

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

    fetch_and_save() >> PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_meteo_to_bronze,
    ) >> SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="snowflake_platform",
        sql=TRANSFORM_METEO_SQL,
    ) >> TriggerDagRunOperator(
        task_id="trigger_dbt_snowflake",
        trigger_dag_id="dbt_platform_snowflake",
        wait_for_completion=False,
    )


open_meteo_snowflake = open_meteo_snowflake_dag()