from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from spotify_etl import run_simple_compare_result_for_bucket_ranges_etl, run_read_data_etl

with DAG(
        "one_simple_spotify_dag_large",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="A spotify DAG",
        schedule=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['simple', 'sequential'],
) as dag:
    t0 = PythonOperator(
        task_id='t0',
        python_callable=run_read_data_etl,
        dag=dag,
    )
    t1 = PythonOperator(
        task_id='t1',
        python_callable=run_simple_compare_result_for_bucket_ranges_etl,
        dag=dag,
    )

    t0 >> t1
