import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from test_etl import run_start, run_sleep
from spotify_etl import run_simple_compare_result_for_bucket_ranges_etl, run_read_data_etl

with DAG(
        "test_short_dag",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="A simple spotify DAG",
        schedule=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["simple_spotify", '2mb_dataset'],
) as dag:
    t0 = PythonOperator(
        task_id='t0',
        python_callable=run_start,
        dag=dag,
    )
    t1 = PythonOperator(
        task_id='t1',
        python_callable=run_sleep,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id='t2',
        python_callable=run_sleep,
        dag=dag,
    )
    t3 = PythonOperator(
        task_id='t3',
        python_callable=run_sleep,
        dag=dag,
    )
    t4 = PythonOperator(
        task_id='t4',
        python_callable=run_sleep,
        dag=dag,
    )

    t0 >> [t1, t2, t3, t4]
