from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.hooks.base import BaseHook
import os
import subprocess
def extract_tmdb_data():
    API_KEY = Variable.get("tmdb_api_key")

    url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={API_KEY}"
    response = requests.get(url)
    data = response.json()

    movies = data["results"]

    df = pd.DataFrame(movies)
    #timestamp for time-series tracking
    df["snapshot_timestamp"] = datetime.now()
    df = df.rename(columns={"id": "tmdb_id"})
    df = df[[
        "tmdb_id",
        "title",
        "release_date",
        "popularity",
        "vote_average",
        "vote_count",
        "genre_ids",
        "original_language", 
        "snapshot_timestamp"
    ]]
    
    print("Preview of TMDb data:")
    print(df.head())

    # temporary storage (for now)
    df.to_csv("/tmp/tmdb_data.csv", index=False)

    print("TMDb realtime ingestion complete!")

def extract_tmdb_genres():
    API_KEY = Variable.get("tmdb_api_key")

    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}"
    data = requests.get(url).json()

    df = pd.DataFrame(data["genres"]).rename(columns={
        "id": "genre_id",
        "name": "genre_name"
    })

    df.to_csv("/tmp/tmdb_genres.csv", index=False)

def load_tmdb():
    hook = SnowflakeHook(snowflake_conn_id="conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    airflow_conn = BaseHook.get_connection("conn")
    extra = airflow_conn.extra_dejson

    warehouse = extra.get("warehouse")
    database = extra.get("database")
    schema = extra.get("schema") or airflow_conn.schema

    try:
        if warehouse:
            cur.execute(f"USE WAREHOUSE {warehouse}")
        if database:
            cur.execute(f"USE DATABASE {database}")
        if schema:
            cur.execute(f"USE SCHEMA {schema}")

        cur.execute("""
            CREATE STAGE IF NOT EXISTS TMDB_STAGE
            FILE_FORMAT = (
                TYPE = 'CSV'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            );
        """)

        cur.execute("""
            PUT file:///tmp/tmdb_data.csv
            @TMDB_STAGE
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE;
        """)

        cur.execute("""
    COPY INTO TMDB_TRENDING
    FROM @TMDB_STAGE/tmdb_data.csv
    FILE_FORMAT = (
        TYPE = 'CSV'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    )
    FORCE = TRUE;
""")

    finally:
        cur.close()
        conn.close()

def load_tmdb_genres():
    hook = SnowflakeHook(snowflake_conn_id="conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    airflow_conn = BaseHook.get_connection("conn")
    extra = airflow_conn.extra_dejson

    warehouse = extra.get("warehouse")
    database = extra.get("database")
    schema = extra.get("schema") or airflow_conn.schema

    try:
        if warehouse:
            cur.execute(f"USE WAREHOUSE {warehouse}")
        if database:
            cur.execute(f"USE DATABASE {database}")
        if schema:
            cur.execute(f"USE SCHEMA {schema}")

            cur.execute("""
    CREATE STAGE IF NOT EXISTS TMDB_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    );
""")

        cur.execute("""
            PUT file:///tmp/tmdb_genres.csv
            @TMDB_STAGE
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE;
        """)

        cur.execute("""
    COPY INTO TMDB_GENRES
    FROM @TMDB_STAGE/tmdb_genres.csv
    FILE_FORMAT = (
        TYPE = 'CSV'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    )
    FORCE = TRUE;
""")

    finally:
        cur.close()
        conn.close()
import os
import subprocess

def run_dbt():
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = "/opt/airflow/dbt/netflix"

    subprocess.run(
        ["dbt", "run"],
        cwd="/opt/airflow/dbt/netflix",
        env=env,
        check=True
    )
with DAG(
    dag_id="tmdb_realtime_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # simulate real-time
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_tmdb_data",
        python_callable=extract_tmdb_data
    )

    load_task = PythonOperator(
    task_id="load_tmdb",
    python_callable=load_tmdb
    )
    extract_genres_task = PythonOperator(
        task_id="extract_tmdb_genres",
        python_callable=extract_tmdb_genres
    )

    load_genres_task = PythonOperator(
        task_id="load_tmdb_genres",
        python_callable=load_tmdb_genres
    )
    dbt_task = PythonOperator(
    task_id="run_dbt",
    python_callable=run_dbt
    )

    extract_task >> load_task
    extract_genres_task >> load_genres_task
    [load_task, load_genres_task] >> dbt_task