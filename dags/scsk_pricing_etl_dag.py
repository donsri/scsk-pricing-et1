from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add your project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scsk_pricing_etl_pipeline',
    default_args=default_args,
    description='Product Pricing ETL Pipeline',
    schedule=timedelta(days=1),  # Changed from schedule_interval
    catchup=False,
    tags=['etl', 'pricing', 'azure']
)

def run_ingest(**context):
    print("Starting ingestion...")
    print("Current working directory:", os.getcwd())
    print("Python path:", sys.path[:3])  # Show first 3 paths
    
    from etl.ingest import fetch_and_store_raw
    print("Import successful, calling function...")
    
    result = fetch_and_store_raw()
    print(f"Function completed with result: {result}")
    return result

def run_transform(**context):
    from etl.transform import transform_to_parquet
    print("Starting transformation...")
    result = transform_to_parquet()
    return result

def run_load(**context):
    from etl.load import load_pipeline
    print("Starting load...")
    result = load_pipeline()
    return result

def pipeline_summary(**context):
    print("Pipeline completed successfully!")
    return {"status": "success"}

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=run_ingest,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=run_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=run_load,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='pipeline_summary',
    python_callable=pipeline_summary,
    dag=dag,
)

ingest_task >> transform_task >> load_task >> summary_task
