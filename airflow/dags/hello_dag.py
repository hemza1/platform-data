import os
import json
from datetime import datetime ,timezone

import requests
from airflow.sdk import dag, task


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
    dag_id="hello_world_simple",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["seance2", "taskflow"],
)
def hello_world_dag():
    @task
    def say_hello() -> str:
        msg = "Hello, Airflow 3.x!"
        print(msg)
        return msg

    @task
    def consume_message(message: str) -> None:
        print(f"Consumed message: {message}")

    consume_message(say_hello))


hello_world = hello_world_dag()
