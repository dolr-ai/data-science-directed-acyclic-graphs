from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}


# Your SQL query as a string
create_embed_query = """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.test_yral_video.video_embeddings` AS (SELECT *
FROM ML.GENERATE_EMBEDDING(
  MODEL `hot-or-not-feed-intelligence.test_yral_video.mm_embed`,
  TABLE `hot-or-not-feed-intelligence.test_yral_video.video_object_table`,
  STRUCT(TRUE AS flatten_json_output,
    10 AS interval_seconds)
));
"""


def print_data(**context):
    rows = context["task_instance"].xcom_pull(task_ids="get_data_from_bq")
    cnt = 0
    for row in rows:
        print(row)
        cnt += 1
        if cnt > 100:
            break


with DAG(
    "video_embed_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    description="A temporary DAG to test video embedding pipeline",
    schedule_interval=None,
    #  max_active_runs=1,
    #  schedule_interval='0 12 8-14,22-28 * 6',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    #  default_args=default_args,
    #  catchup=False # enable if you don't want historical dag runs to run
) as dag:

    # run_query = BigQueryExecuteQueryOperator(
    #     task_id="run_query",
    #     sql=create_embed_query,
    #     use_legacy_sql=False,
    #     dag=dag,
    # )

    get_data = BigQueryGetDataOperator(
        task_id="get_data_from_bq",
        dataset_id="hot-or-not-feed-intelligence.test_yral_video",
        table_id="hot-or-not-feed-intelligence.test_yral_video.video_embeddings",
        max_results=500000,
        selected_fields="uri",
    )

    # Task to print the data
    print_data_task = PythonOperator(
        task_id="print_data",
        provide_context=True,
        python_callable=print_data,
    )

    get_data >> print_data_task

    # run_query