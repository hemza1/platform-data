import os
import sys
from pathlib import Path

# IMPORTANT: on pointe vers le dossier de DAGs du repo
DAGS_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"

def main() -> int:
    if not DAGS_FOLDER.exists():
        print(f"ERROR: DAGS folder not found: {DAGS_FOLDER}")
        return 1

    # Airflow veut AIRFLOW_HOME
    airflow_home = Path(__file__).resolve().parents[1] / ".airflow_home_ci"
    airflow_home.mkdir(exist_ok=True)
    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"

    # Parse DAGs
    from airflow.models.dagbag import DagBag  # noqa: E402

    dagbag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)

    if dagbag.import_errors:
        print("DAG IMPORT ERRORS:")
        for file, err in dagbag.import_errors.items():
            print(f"\n--- {file} ---\n{err}")
        return 1

    print(f"OK: {len(dagbag.dags)} DAG(s) parsed from {DAGS_FOLDER}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
