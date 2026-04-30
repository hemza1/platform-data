from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote_plus

import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

DVF_URL = "https://www.data.gouv.fr/api/1/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
OUT_PATH = Path("/opt/airflow/data/dvf/2025/valeursfoncieres-2025.txt.zip")

TRANSFORM_DVF_SQL = """
CREATE OR REPLACE TABLE PLATFORM_DB.SILVER.dvf_mutations_gold AS
SELECT
    TRY_TO_DATE(date_mutation, 'DD/MM/YYYY')               AS date_mutation,
    TRY_TO_NUMERIC(REPLACE(valeur_fonciere, ',', '.'))      AS valeur_fonciere,
    code_postal,
    commune,
    code_departement,
    type_local,
    TRY_TO_NUMERIC(REPLACE(surface_reelle_bati, ',', '.'))  AS surface_reelle_bati,
    TRY_TO_NUMBER(nombre_pieces_principales)                AS nombre_pieces_principales,
    TRY_TO_NUMERIC(REPLACE(surface_terrain, ',', '.'))      AS surface_terrain,
    nature_mutation
FROM PLATFORM_DB.BRONZE.dvf_2025_s1
WHERE valeur_fonciere IS NOT NULL
  AND valeur_fonciere <> ''
;
"""


def notify_failure(context):
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return
    ti = context["ti"]
    payload = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "state": "failed",
    }
    requests.post(url, data=json.dumps(payload),
                  headers={"Content-Type": "application/json"}, timeout=10)


def load_dvf_to_bronze():
    import pandas as pd
    from sqlalchemy import create_engine, text
    from airflow.sdk.bases.hook import BaseHook

    if not OUT_PATH.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {OUT_PATH}")

    df = pd.read_csv(
        OUT_PATH,
        sep="|",
        compression="zip",
        dtype=str,
        low_memory=False,
    )

    df.columns = [
        c.strip().lower().replace(" ", "_").replace("-", "_").replace("'", "")
        for c in df.columns
    ]

    cols_to_keep = [
        "id_mutation", "date_mutation", "nature_mutation", "valeur_fonciere",
        "code_postal", "commune", "code_departement", "type_local",
        "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain",
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]]
    df.columns = [c.upper() for c in df.columns]

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

    # Drop table manually to avoid pandas reflect issue on first run
    with engine.connect() as con:
        con.execute(text('DROP TABLE IF EXISTS PLATFORM_DB.BRONZE."DVF_2025_S1"'))
        con.commit()

    df.to_sql(
        "DVF_2025_S1",
        engine,
        schema="BRONZE",
        if_exists="append",
        index=False,
        chunksize=10000,
    )
    print(f"{len(df)} lignes chargées dans PLATFORM_DB.BRONZE.DVF_2025_S1")


@dag(
    dag_id="extract_dvf_2025_s1_snowflake",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "dvf", "bronze"],
)
def dvf_2025_snowflake_dag():

    @task(retries=3, retry_delay=timedelta(seconds=30), on_failure_callback=notify_failure)
    def download_dvf() -> str:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = OUT_PATH.with_suffix(OUT_PATH.suffix + ".part")
        dvf_url = Variable.get("DVF_URL", default_var=DVF_URL)

        with requests.get(dvf_url, stream=True, timeout=(10, 300)) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        tmp_path.replace(OUT_PATH)
        print(f"DVF téléchargé : {OUT_PATH}")
        return str(OUT_PATH)

    download_dvf() >> PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_dvf_to_bronze,
    ) >> SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="snowflake_platform",
        sql=TRANSFORM_DVF_SQL,
    ) >> TriggerDagRunOperator(
        task_id="trigger_dbt_snowflake",
        trigger_dag_id="dbt_platform_snowflake",
        wait_for_completion=False,
    )


dvf_2025_snowflake = dvf_2025_snowflake_dag()