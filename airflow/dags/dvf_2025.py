# airflow/dags/dvf_2025.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow.sdk import dag, task
import requests 

DVF_URL = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20251018-234902/valeursfoncieres-2025-s1.txt.zip"
OUT_PATH = Path("/opt/airflow/data/dvf/2025/valeursfoncieres-2025-s1.txt.zip")


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


@dag(
    dag_id="extract_dvf_2025_s1",
    schedule="@weekly",  # ou None si tu veux uniquement en manuel
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["seance2", "extraction", "dvf"],
)
def dvf_2025_dag():
    @task(retries=3)
    def download_dvf() -> str:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        tmp_path = OUT_PATH.with_suffix(OUT_PATH.suffix + ".part")

        with requests.get(DVF_URL, stream=True, timeout=(10, 300)) as r:
            r.raise_for_status()

            # Téléchargement en chunks (important si fichier lourd)
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MiB
                    if chunk:
                        f.write(chunk)

        tmp_path.replace(OUT_PATH)

        size_mb = OUT_PATH.stat().st_size / (1024 * 1024)
        print(f"DVF téléchargé: {OUT_PATH} ({size_mb:.2f} MiB)")
        return str(OUT_PATH)

    download_dvf()


dvf_2025 = dvf_2025_dag()
