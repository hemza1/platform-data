# airflow/dags/open_meteo.py
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from airflow.sdk import dag, task

OUT_DIR = Path("/opt/airflow/data/meteo")

# Exemple: Marseille (tes paramètres)
LAT = 43.297
LON = 5.3811

API_URL = "https://api.open-meteo.com/v1/forecast"


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
            "timezone": "auto",  # tu peux mettre "Europe/Paris" si tu préfères
        }

        r = requests.get(API_URL, params=params, timeout=(10, 60))
        r.raise_for_status()
        payload = r.json()

        out_path = OUT_DIR / "marseille_forecast.json"
        tmp_path = out_path.with_suffix(".json.part")

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        tmp_path.replace(out_path)

        print(f"Météo sauvegardée: {out_path}")
        return str(out_path)

    fetch_and_save()


open_meteo = open_meteo_dag()
