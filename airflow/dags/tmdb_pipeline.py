from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import pandas as pd

def extract_tmdb_data():
    API_KEY = Variable.get("tmdb_api_key")

    url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={API_KEY}"
    response = requests.get(url)
    data = response.json()

    movies = data["results"]

    df = pd.DataFrame(movies)

    df = df[[
        "id",
        "title",
        "release_date",
        "popularity",
        "vote_average"
    ]]

    #timestamp for time-series tracking
    df["snapshot_timestamp"] = datetime.now()

    print("Preview of TMDb data:")
    print(df.head())

    # temporary storage (for now)
    df.to_csv("/tmp/tmdb_data.csv", index=False)

    print("TMDb realtime ingestion complete!")

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