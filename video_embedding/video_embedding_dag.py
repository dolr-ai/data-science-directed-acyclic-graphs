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
    "retries": 0,
    # "depends_on_past": True,
}


# Your SQL query as a string
create_embed_query = """
INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_embeddings` 
SELECT
  *
FROM ML.GENERATE_EMBEDDING(
  MODEL `hot-or-not-feed-intelligence.yral_ds.mm_embed`,
  (SELECT * FROM `hot-or-not-feed-intelligence.yral_ds.video_object_table` WHERE uri NOT IN (SELECT uri FROM `hot-or-not-feed-intelligence.yral_ds.video_embeddings`) LIMIT 5),
  STRUCT(TRUE AS flatten_json_output, 10 AS interval_seconds)
);
"""


with DAG(
    "video_embed_pipeline_dag",
    default_args=default_args,
    description="DAG for video embedding pipeline. Runs every hour",
    # schedule_interval="0 * * * *",
    max_active_runs=1,  # Ensures only one active run at a time
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # run_create_embed_query = BigQueryExecuteQueryOperator(
    #     task_id="run_query",
    #     sql=create_embed_query,
    #     use_legacy_sql=False,
    #     dag=dag,
    # )

    def run_create_embed_query(**kwargs):
        hook = BigQueryHook(use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(create_embed_query)

    run_query = PythonOperator(
        task_id="run_create_embed_query",
        provide_context=True,
        python_callable=run_create_embed_query,
    )

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
        run_query
        # >> fetch_data
        # >> process_uris
        # >> transfer_and_delete_gcs_objects
    )
