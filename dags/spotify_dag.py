from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from spotify_etl import HIGH, run_compare_genres_count_for_loudness_ranges_etl, \
    run_count_genres_within_loudness_range_etl, run_calculate_loudness_ranges_etl, MIDDLE, LOW, run_end_etl

with DAG(
        "spotify",
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
        tags=["spotify"]
) as dag:
    t1 = PythonOperator(
        task_id='t1',
        python_callable=run_calculate_loudness_ranges_etl,
        dag=dag,
        op_args=[LOW]
    )
    t2 = PythonOperator(
        task_id='t2',
        python_callable=run_count_genres_within_loudness_range_etl,
        dag=dag,
        op_args=[MIDDLE]
    )
    t3 = PythonOperator(
        task_id='t3',
        python_callable=run_count_genres_within_loudness_range_etl,
        dag=dag,
        op_args=[HIGH]
    )
    t4 = PythonOperator(
        task_id='t4',
        python_callable=run_count_genres_within_loudness_range_etl,
        dag=dag,
    )
    t5 = PythonOperator(
        task_id='t5',
        python_callable=run_compare_genres_count_for_loudness_ranges_etl,
        dag=dag,
    )
    t6 = PythonOperator(
        task_id='t6',
        python_callable=run_end_etl,
        dag=dag,
    )

    t1 >> [t2, t3, t4] >> t5 >> t6
