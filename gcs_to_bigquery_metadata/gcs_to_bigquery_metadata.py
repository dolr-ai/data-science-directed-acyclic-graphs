from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests

default_args = {
    "owner": "airflow",
    "retries": 0,
    # "depends_on_past": True,
}


with DAG(
    "gcs_to_bigquery_metadata_dag",
    default_args=default_args,
    description="DAG for metadata enrichment",
    schedule_interval=None,
    max_active_runs=1,  # Ensures only one active run at a time
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def enrich_metadata(**kwargs):
        hook = GCSHook()
        obj_list = hook.list("yral-videos")

        for obj in obj_list[:10]:
            print(obj)

    enrich_objects = PythonOperator(
        task_id="enrich_objects",
        provide_context=True,
        python_callable=enrich_metadata,
    )

    (enrich_objects)
