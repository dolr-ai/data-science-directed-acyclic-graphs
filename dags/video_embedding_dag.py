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

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}


# Your SQL query as a string
create_embed_query = """
INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_embeddings` 
SELECT
  *
FROM ML.GENERATE_EMBEDDING(
  MODEL `hot-or-not-feed-intelligence.yral_ds.mm_embed`,
  (SELECT * FROM `hot-or-not-feed-intelligence.yral_ds.video_object_table` WHERE uri NOT IN (SELECT uri FROM `hot-or-not-feed-intelligence.yral_ds.video_embeddings`) LIMIT 10000),
  STRUCT(TRUE AS flatten_json_output, 10 AS interval_seconds)
);
"""


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

    run_create_embed_query = BigQueryExecuteQueryOperator(
        task_id="run_query",
        sql=create_embed_query,
        use_legacy_sql=False,
        dag=dag,
    )

    # def get_data_from_bq(**kwargs):
    #     hook = BigQueryHook(use_legacy_sql=False)
    #     conn = hook.get_conn()
    #     cursor = conn.cursor()
    #     cursor.execute(
    #         "SELECT DISTINCT uri FROM `hot-or-not-feed-intelligence.yral_ds.video_object_table` WHERE uri IN (SELECT uri FROM `hot-or-not-feed-intelligence.yral_ds.video_embeddings`);"
    #     )
    #     result = cursor.fetchall()
    #     return result

    # fetch_data = PythonOperator(
    #     task_id="fetch_data_from_bq",
    #     provide_context=True,
    #     python_callable=get_data_from_bq,
    # )

    # def extract_object_names(**kwargs):
    #     task_instance = kwargs["task_instance"]
    #     obj_table_rows = task_instance.xcom_pull(task_ids="fetch_data_from_bq")
    #     # print("obj_table_rows", obj_table_rows)

    #     object_names = []
    #     for obj in obj_table_rows:
    #         uri = obj[0]
    #         object_name = uri.split("gs://")[1].split("/", 1)[1]
    #         object_names.append(object_name)

    #     # Push the list of object names to XCom
    #     task_instance.xcom_push(key="object_names", value=object_names)

    # process_uris = PythonOperator(
    #     task_id="process_uris",
    #     python_callable=extract_object_names,
    #     provide_context=True,
    # )

    # def transfer_and_delete_objs(**kwargs):
    #     hook = GCSHook()
    #     task_instance = kwargs["task_instance"]
    #     array_objects = task_instance.xcom_pull(
    #         task_ids="process_uris", key="object_names"
    #     )

    #     array_objects = list(set(array_objects))

    #     for arr in array_objects:
    #         # hook.copy(
    #         #     source_bucket="yral-videos",
    #         #     source_object=arr,
    #         #     destination_bucket="yral-videos-backup",
    #         #     destination_object=arr,
    #         # )
    #         hook.delete(bucket_name="yral-videos", object_name=arr)

    # transfer_and_delete_gcs_objects = PythonOperator(
    #     task_id="delete_gcs_obj",
    #     provide_context=True,
    #     python_callable=transfer_and_delete_objs,
    # )

    # Define task dependencies
    (
        run_create_embed_query
        # >> fetch_data
        # >> process_uris
        # >> transfer_and_delete_gcs_objects
    )
