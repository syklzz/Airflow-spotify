from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from spotify_etl import run_simple_compare_genres_count_for_loudness_ranges_etl, run_end_etl

with DAG(
        "simple_spotify",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        schedule=timedelta(days=1),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["simple_spotify"]
) as dag:
    t1 = PythonOperator(
        task_id='t1',
        python_callable=run_simple_compare_genres_count_for_loudness_ranges_etl,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id='t2',
        python_callable=run_end_etl,
        dag=dag,
    )

    t1 >> t2
