# airflow/dags/dvf_2025.py
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

DVF_URL = "https://www.data.gouv.fr/api/1/datasets/r/4d741143-8331-4b59-95c2-3b24a7bdbe3c"
OUT_PATH = Path("/opt/airflow/data/dvf/2025/valeursfoncieres-2025-s1.txt.zip")


TRANSFORM_DVF_SQL = """
DROP TABLE IF EXISTS silver.dvf_2025_s1;

CREATE TABLE silver.dvf_2025_s1 AS
SELECT
    TO_DATE(date_mutation, 'DD/MM/YYYY') AS date_mutation,

    CASE
        WHEN valeur_fonciere IS NULL OR valeur_fonciere = ''
            THEN NULL
        ELSE REPLACE(valeur_fonciere, ',', '.')::numeric
    END AS valeur_fonciere,

    code_postal,
    commune,
    code_departement,
    type_local,

    CASE
        WHEN surface_reelle_bati IS NULL OR surface_reelle_bati = ''
            THEN NULL
        ELSE REPLACE(surface_reelle_bati, ',', '.')::numeric
    END AS surface_reelle_bati,

    CASE
        WHEN nombre_pieces_principales IS NULL OR nombre_pieces_principales = ''
            THEN NULL
        ELSE nombre_pieces_principales::int
    END AS nombre_pieces_principales,

    CASE
        WHEN surface_terrain IS NULL OR surface_terrain = ''
            THEN NULL
        ELSE REPLACE(surface_terrain, ',', '.')::numeric
    END AS surface_terrain,

    nature_mutation
FROM bronze.dvf_2025_s1
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
    requests.post(
        url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )


def load_dvf_to_bronze():
    """Lecture du fichier zip DVF -> chargement dans bronze.dvf_2025_s1."""
    import pandas as pd
    from sqlalchemy import create_engine

    if not OUT_PATH.exists():
        raise FileNotFoundError(f"Fichier non trouvé : {OUT_PATH}")

    # DVF est un fichier texte zippé ; on charge tout en texte
    df = pd.read_csv(
        OUT_PATH,
        sep="|",
        compression="zip",
        dtype=str,
        low_memory=False,
    )

    # Normalisation des noms de colonnes pour simplifier le SQL
    df.columns = [
        c.strip()
         .lower()
         .replace(" ", "_")
         .replace("-", "_")
         .replace("'", "")
        for c in df.columns
    ]

    # On garde seulement un sous-ensemble utile pour démarrer
    cols_to_keep = [
        "id_mutation",
        "date_mutation",
        "nature_mutation",
        "valeur_fonciere",
        "code_postal",
        "commune",
        "code_departement",
        "type_local",
        "surface_reelle_bati",
        "nombre_pieces_principales",
        "surface_terrain",
    ]

    existing_cols = [c for c in cols_to_keep if c in df.columns]
    df = df[existing_cols]

    conn = BaseHook.get_connection("postgres_warehouse")
    port = conn.port or 5432
    dbname = conn.schema or "warehouse"
    engine = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{port}/{dbname}"
    )

    # Comme la source est toujours le même fichier, on remplace pour éviter les doublons
    df.to_sql(
        "dvf_2025_s1",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
    )

    print(f"{len(df)} lignes chargées dans bronze.dvf_2025_s1")


def fetch_dvf() -> str:
    """Téléchargement DVF — importable par d'autres DAGs."""
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


@dag(
    dag_id="extract_dvf_2025_s1",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["seance3", "extract", "load", "transform", "dvf"],
)
def dvf_2025_dag():
    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        on_failure_callback=notify_failure,
    )
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

    task_download = download_dvf()

    load_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_dvf_to_bronze,
    )

    transform_silver = SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="postgres_warehouse",
        sql=TRANSFORM_DVF_SQL,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_platform",
        trigger_dag_id="dbt_platform",
        wait_for_completion=False,
    )

    task_download >> load_bronze >> transform_silver >> trigger_dbt


dvf_2025 = dvf_2025_dag()