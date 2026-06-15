"""Pipeline E2E Snowflake : extraction, stage raw, bronze, puis dbt."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag

try:
    from pipeline_tasks import (
        fetch_dvf,
        fetch_meteo,
        prepare_dvf_for_stage,
        put_dvf_to_raw_stage,
        put_meteo_to_raw_stage,
    )
except ModuleNotFoundError:
    dags_dir = str(Path(__file__).resolve().parent)
    if dags_dir not in sys.path:
        sys.path.append(dags_dir)
    from pipeline_tasks import (  # noqa: E402
        fetch_dvf,
        fetch_meteo,
        prepare_dvf_for_stage,
        put_dvf_to_raw_stage,
        put_meteo_to_raw_stage,
    )

SNOWFLAKE_CONN_ID = "snowflake_platform"

LOAD_DVF_SQL = [
    """
    CREATE FILE FORMAT IF NOT EXISTS PLATFORM_DB.BRONZE.DVF_PIPE_FORMAT
        TYPE = CSV
        FIELD_DELIMITER = '|'
        SKIP_HEADER = 1
        NULL_IF = ('', 'NULL')
        EMPTY_FIELD_AS_NULL = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    """,
    """
    CREATE TABLE IF NOT EXISTS PLATFORM_DB.BRONZE.DVF_2025_S1 (
        date_mutation VARCHAR,
        valeur_fonciere VARCHAR,
        code_postal VARCHAR,
        commune VARCHAR,
        code_departement VARCHAR,
        type_local VARCHAR,
        surface_reelle_bati VARCHAR,
        nombre_pieces_principales VARCHAR,
        surface_terrain VARCHAR,
        nature_mutation VARCHAR
    )
    """,
    "TRUNCATE TABLE PLATFORM_DB.BRONZE.DVF_2025_S1",
    """
    COPY INTO PLATFORM_DB.BRONZE.DVF_2025_S1 (
        date_mutation,
        valeur_fonciere,
        code_postal,
        commune,
        code_departement,
        type_local,
        surface_reelle_bati,
        nombre_pieces_principales,
        surface_terrain,
        nature_mutation
    )
    FROM (
        SELECT
            t.$9,
            t.$11,
            t.$17,
            t.$18,
            t.$19,
            t.$37,
            t.$39,
            t.$40,
            t.$43,
            t.$10
        FROM @PLATFORM_DB.BRONZE.RAW_STAGE/dvf/annee=2025/
            (FILE_FORMAT => 'PLATFORM_DB.BRONZE.DVF_PIPE_FORMAT') t
    )
    PATTERN = '.*dvf_2025[.]txt'
    FORCE = TRUE
    ON_ERROR = 'CONTINUE'
    """,
]

LOAD_METEO_SQL = [
    """
    CREATE FILE FORMAT IF NOT EXISTS PLATFORM_DB.BRONZE.METEO_JSON_FORMAT
        TYPE = JSON
        STRIP_OUTER_ARRAY = FALSE
    """,
    """
    CREATE TABLE IF NOT EXISTS PLATFORM_DB.BRONZE.METEO_RAW (
        payload VARIANT,
        source_filename VARCHAR,
        loaded_at TIMESTAMP_TZ
    )
    """,
    "TRUNCATE TABLE PLATFORM_DB.BRONZE.METEO_RAW",
    """
    COPY INTO PLATFORM_DB.BRONZE.METEO_RAW (
        payload,
        source_filename,
        loaded_at
    )
    FROM (
        SELECT
            t.$1,
            METADATA$FILENAME,
            CURRENT_TIMESTAMP()
        FROM @PLATFORM_DB.BRONZE.RAW_STAGE/meteo/
            (FILE_FORMAT => 'PLATFORM_DB.BRONZE.METEO_JSON_FORMAT') t
    )
    PATTERN = '.*marseille_forecast[.]json'
    FORCE = TRUE
    """,
    """
    CREATE OR REPLACE TABLE PLATFORM_DB.BRONZE.METEO_QUOTIDIEN AS
    WITH observations AS (
        SELECT
            dates.value::VARCHAR AS date_meteo,
            codes.value::VARCHAR AS weather_code,
            raw.source_filename,
            raw.loaded_at
        FROM PLATFORM_DB.BRONZE.METEO_RAW raw,
        LATERAL FLATTEN(INPUT => raw.payload:daily:time) dates,
        LATERAL FLATTEN(INPUT => raw.payload:daily:weather_code) codes
        WHERE dates.index = codes.index
    )
    SELECT
        date_meteo,
        weather_code
    FROM observations
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date_meteo
        ORDER BY loaded_at DESC, source_filename DESC
    ) = 1
    """,
]


@dag(
    dag_id="elt_snowflake",
    description="E2E Snowflake: extract, PUT raw, COPY bronze, dbt silver/gold",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["e2e", "elt", "snowflake", "seance7"],
)
def elt_snowflake_dag():
    extract_dvf = PythonOperator(
        task_id="extract_dvf",
        python_callable=fetch_dvf,
    )
    prepare_dvf = PythonOperator(
        task_id="prepare_dvf_for_stage",
        python_callable=prepare_dvf_for_stage,
    )
    put_dvf = PythonOperator(
        task_id="put_dvf_to_raw_stage",
        python_callable=put_dvf_to_raw_stage,
    )
    copy_dvf = SQLExecuteQueryOperator(
        task_id="copy_dvf_to_bronze",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_DVF_SQL,
        autocommit=True,
    )

    extract_meteo = PythonOperator(
        task_id="extract_meteo",
        python_callable=fetch_meteo,
    )
    put_meteo = PythonOperator(
        task_id="put_meteo_to_raw_stage",
        python_callable=put_meteo_to_raw_stage,
    )
    copy_meteo = SQLExecuteQueryOperator(
        task_id="copy_meteo_to_bronze",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_METEO_SQL,
        autocommit=True,
    )

    run_dbt_snowflake = TriggerDagRunOperator(
        task_id="run_dbt_snowflake",
        trigger_dag_id="dbt_platform_snowflake",
        trigger_run_id="elt_snowflake__{{ run_id }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15,
    )

    extract_dvf >> prepare_dvf >> put_dvf >> copy_dvf
    extract_meteo >> put_meteo >> copy_meteo
    [copy_dvf, copy_meteo] >> run_dbt_snowflake


elt_snowflake = elt_snowflake_dag()
