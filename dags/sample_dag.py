from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Define the functions for each task
def ingest_data_function():
    print("Ingesting data...")
    # Simulate some work
    import time
    time.sleep(5)
    print("Data ingested successfully.")

def transform_data_function():
    print("Transforming data...")
    # Simulate some work
    import time
    time.sleep(5)
    print("Data transformed successfully.")

def load_data_function():
    print("Loading data...")
    # Simulate some work
    import time
    time.sleep(5)
    print("Data loaded successfully.")

def pipeline_summary_function():
    print("Pipeline summary complete.")

with DAG(
    dag_id="sample_test_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # Run manually
    catchup=False,
    tags=["test", "python"],
) as dag:
    # Define the tasks
    ingest_data_task = PythonOperator(
        task_id="ingest_data_task",
        python_callable=ingest_data_function,
    )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data_function,
    )

    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data_function,
    )

    pipeline_summary_task = PythonOperator(
        task_id="pipeline_summary_task",
        python_callable=pipeline_summary_function,
    )

    # Define the task dependencies
    ingest_data_task >> transform_data_task >> load_data_task >> pipeline_summary_task