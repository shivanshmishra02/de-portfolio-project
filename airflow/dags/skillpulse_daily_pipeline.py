from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG nodes
default_args = {
    'owner': 'skillpulse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'skillpulse_daily_pipeline',
    default_args=default_args,
    description='Automated job market intelligence data pipeline (Medallion Architecture)',
    schedule_interval='30 0 * * *', # 6:00 AM IST (00:30 UTC)
    start_date=datetime(2026, 3, 9), # Start tracking realistically
    catchup=False,
    tags=['skillpulse', 'medallion', 'genai'],
) as dag:

    # Task 1: Scrape Bronze data using JSearch API
    ingest_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='cd /opt/airflow && python -m src.ingestion.fetch_jobs_bronze',
    )

    # Task 2: AI Enrich Silver data via Gemini
    enrich_silver = BashOperator(
        task_id='enrich_silver',
        bash_command='cd /opt/airflow && python -m src.enrichment.process_silver',
    )

    # Task 3: Load valid enriched records into BigQuery target
    load_to_bq = BashOperator(
        task_id='load_to_bq',
        bash_command='cd /opt/airflow && python -m src.warehouse.load_silver_to_bq',
    )

    # Task 4: Execute dbt transformations on Gold layer (fact + dimensions + scd)
    # Adding a dbt deps command seamlessly ensures the bigquery wrapper and dbt_utils package resolve inside container
    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='cd /opt/airflow/dbt_project && dbt deps && dbt build --target dev',
    )

    # Execution Flow definitions
    ingest_bronze >> enrich_silver >> load_to_bq >> dbt_build
