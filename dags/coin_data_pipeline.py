from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import pipeline functions
from pipelines.extract import extract_data
from pipelines.bronze_layer import upload_raw_to_s3
from pipelines.silver_layer import transform_bronze_to_silver
from pipelines.gold_layer import build_gold_coin_daily_minio
from pipelines.warehouse_load import load_gold_to_postgres

# Import validations
from utils.validation_utils import (
    validate_gold_metrics,
    validate_gold_row_count,
    validate_gold_sanity,
    validate_gold_freshness,
    validate_gold_sla
)

# Import database helper
from utils.db_utils import create_tables


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


dag = DAG(
    dag_id="coin_data_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    description="Crypto Data Platform Pipeline",
)


# -----------------------------
# Tasks
# -----------------------------

create_tables_task = PythonOperator(
    task_id="create_tables",
    python_callable=create_tables,
    dag=dag,
)


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)


bronze_task = PythonOperator(
    task_id="upload_raw_to_s3",
    python_callable=upload_raw_to_s3,
    provide_context=True,
    dag=dag,
)


silver_task = PythonOperator(
    task_id="transform_bronze_to_silver",
    python_callable=transform_bronze_to_silver,
    provide_context=True,
    dag=dag,
)


gold_task = PythonOperator(
    task_id="build_gold_minio",
    python_callable=build_gold_coin_daily_minio,
    provide_context=True,
    dag=dag,
)


warehouse_task = PythonOperator(
    task_id="load_gold_postgres",
    python_callable=load_gold_to_postgres,
    provide_context=True,
    dag=dag,
)


validate_rowcount_task = PythonOperator(
    task_id="validate_gold_row_count",
    python_callable=validate_gold_row_count,
    provide_context=True,
    dag=dag,
)


validate_sanity_task = PythonOperator(
    task_id="validate_gold_sanity",
    python_callable=validate_gold_sanity,
    provide_context=True,
    dag=dag,
)


validate_freshness_task = PythonOperator(
    task_id="validate_gold_freshness",
    python_callable=validate_gold_freshness,
    provide_context=True,
    dag=dag,
)


validate_sla_task = PythonOperator(
    task_id="validate_gold_sla",
    python_callable=validate_gold_sla,
    provide_context=True,
    dag=dag,
)


validate_metrics_task = PythonOperator(
    task_id="validate_gold_metrics",
    python_callable=validate_gold_metrics,
    dag=dag,
)


# -----------------------------
# DAG Dependencies
# -----------------------------

create_tables_task >> extract_task >> bronze_task >> silver_task >> gold_task >> warehouse_task >> validate_rowcount_task >> validate_sanity_task >> validate_freshness_task >> validate_sla_task >> validate_metrics_task