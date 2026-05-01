from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import pandas as pd
import snowflake.connector
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


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
        "original_language"
    ]]
    
    print("Preview of TMDb data:")
    print(df.head())

    # temporary storage (for now)
    df.to_csv("/tmp/tmdb_data.csv", index=False)

    print("TMDb realtime ingestion complete!")

def load_tmdb():
    hook = SnowflakeHook(snowflake_conn_id="conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    # set context
    cur.execute("USE DATABASE USER_DB_GECKO")
    cur.execute("USE SCHEMA RAW")

    # upload file to stage
    cur.execute("""
        PUT file:///tmp/tmdb_data.csv
        @USER_DB_GECKO.RAW.TMDB_STAGE
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE;
    """)

    # load into table (no mapping needed if columns reordered)
    cur.execute("""
        COPY INTO USER_DB_GECKO.RAW.TMDB_TRENDING
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        FORCE = TRUE;
    """)

    cur.close()
    conn.close()

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

extract_task >> load_task