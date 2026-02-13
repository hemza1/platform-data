from datetime import datetime
from airflow.sdk import dag, task


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

    consume_message(say_hello()


hello_world = hello_world_dag()
