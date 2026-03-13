"""Fonctions réutilisables pour les pipelines E/L, sans déclaration de DAG."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import requests
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook

# Meteo
OUT_DIR = Path("/opt/airflow/data/meteo")
LAT = 43.297
LON = 5.3811
API_URL = "https://api.open-meteo.com/v1/forecast"

# DVF
DVF_URL = "https://www.data.gouv.fr/api/1/datasets/r/4d741143-8331-4b59-95c2-3b24a7bdbe3c"
OUT_PATH = Path("/opt/airflow/data/dvf/2025/valeursfoncieres-2025-s1.txt.zip")


def _postgres_engine(conn_id: str = "postgres_warehouse"):
    from sqlalchemy import create_engine

    conn = BaseHook.get_connection(conn_id)
    port = conn.port or 5432
    dbname = conn.schema or "warehouse"
    return create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{port}/{dbname}"
    )


def fetch_meteo() -> str:
    """Extraction Open-Meteo vers un fichier JSON local."""
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
    print(f"Meteo sauvegardee: {out_path}")
    return str(out_path)


def load_meteo_to_bronze():
    """Lecture du JSON meteo puis insertion dans bronze.meteo_quotidien."""
    import pandas as pd

    src = OUT_DIR / "marseille_forecast.json"
    if not src.exists():
        raise FileNotFoundError(f"Fichier non trouve : {src}")

    with open(src, encoding="utf-8") as f:
        data = json.load(f)

    daily = data["daily"]
    df = pd.DataFrame({
        "date_meteo": daily["time"],
        "weather_code": daily["weather_code"],
    })

    engine = _postgres_engine("postgres_warehouse")

    df.to_sql(
        "meteo_quotidien",
        engine,
        schema="bronze",
        if_exists="append",
        index=False,
    )

    print(f"{len(df)} lignes chargees dans bronze.meteo_quotidien")


def fetch_dvf() -> str:
    """Telechargement DVF vers un fichier zip local."""
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
    print(f"DVF telecharge : {OUT_PATH}")
    return str(OUT_PATH)


def load_dvf_to_bronze():
    """Lecture du zip DVF puis chargement dans bronze.dvf_2025_s1."""
    import pandas as pd

    if not OUT_PATH.exists():
        raise FileNotFoundError(f"Fichier non trouve : {OUT_PATH}")

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

    engine = _postgres_engine("postgres_warehouse")

    df.to_sql(
        "dvf_2025_s1",
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
    )

    print(f"{len(df)} lignes chargees dans bronze.dvf_2025_s1")


def put_dvf_to_raw_stage(**context):
    """Depose le fichier DVF brut dans Snowflake RAW_STAGE avec partition annee."""
    import snowflake.connector

    conn_info = BaseHook.get_connection("snowflake_platform")
    extra = conn_info.extra_dejson

    cnx = snowflake.connector.connect(
        account=extra["account"],
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema="BRONZE",
        role=extra.get("role", "ACCOUNTADMIN"),
    )

    data_dir = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
    candidates = [
        data_dir / "dvf" / "dvf_2025.txt",
        data_dir / "dvf" / "2025" / "valeursfoncieres-2025-s1.txt.zip",
    ]
    local_path = next((p for p in candidates if p.exists()), None)
    if not local_path:
        raise FileNotFoundError(
            f"Aucun fichier DVF trouve. Chemins testes: {[str(p) for p in candidates]}"
        )

    cursor = cnx.cursor()
    cursor.execute(
        "PUT file://{src} @PLATFORM_DB.BRONZE.RAW_STAGE/dvf/annee=2025/ "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE".format(src=local_path)
    )
    cursor.close()
    cnx.close()


def put_meteo_to_raw_stage(**context):
    """Depose le dernier fichier meteo JSON dans Snowflake RAW_STAGE avec partition date."""
    import snowflake.connector

    conn_info = BaseHook.get_connection("snowflake_platform")
    extra = conn_info.extra_dejson

    cnx = snowflake.connector.connect(
        account=extra["account"],
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra["warehouse"],
        database=extra["database"],
        schema="BRONZE",
        role=extra.get("role", "ACCOUNTADMIN"),
    )

    today = datetime.now().strftime("%Y-%m-%d")
    data_dir = Path(os.environ.get("DATA_DIR", "/opt/airflow/data")) / "meteo"
    files = sorted(data_dir.glob("*.json"))
    local_path = files[-1] if files else None
    if not local_path:
        raise FileNotFoundError(f"Aucun fichier meteo trouve dans {data_dir}")

    cursor = cnx.cursor()
    cursor.execute(
        "PUT file://{src} @PLATFORM_DB.BRONZE.RAW_STAGE/meteo/date={d}/ "
        "AUTO_COMPRESS=FALSE OVERWRITE=TRUE".format(src=local_path, d=today)
    )
    cursor.close()
    cnx.close()
