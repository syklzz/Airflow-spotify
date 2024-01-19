from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from spotify_etl import run_read_data_etl, \
    run_compare_result_for_bucket_ranges_etl, run_count_within_bucket_range_etl, MIDDLE, HIGH, LOW, \
    run_calculate_bucket_ranges_etl

with DAG(
        "sequential_spotify_dag_large",
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
        tags=["sequential"],
) as dag:
    t0 = PythonOperator(
        task_id='t0',
        python_callable=run_read_data_etl,
        dag=dag,
    )
    t1 = PythonOperator(
        task_id='t1',
        python_callable=run_calculate_bucket_ranges_etl,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id='t2',
        python_callable=run_count_within_bucket_range_etl,
        dag=dag,
        op_args=[LOW]
    )
    t3 = PythonOperator(
        task_id='t3',
        python_callable=run_count_within_bucket_range_etl,
        dag=dag,
        op_args=[MIDDLE]
    )
    t4 = PythonOperator(
        task_id='t4',
        python_callable=run_count_within_bucket_range_etl,
        dag=dag,
        op_args=[HIGH]
    )
    t5 = PythonOperator(
        task_id='t5',
        python_callable=run_compare_result_for_bucket_ranges_etl,
        dag=dag,
    )

    t0 >> t1 >> t2 >> t3 >> t4 >> t5
