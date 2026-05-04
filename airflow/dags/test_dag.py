from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_function():
    print("Airflow is working!")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id="test_task",
        python_callable=test_function
    )