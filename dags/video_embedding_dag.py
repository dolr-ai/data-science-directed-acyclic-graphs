from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}


# Your SQL query as a string
create_embed_query = """
MERGE `hot-or-not-feed-intelligence.test_yral_video.video_embeddings` AS target
USING
(SELECT *
FROM ML.GENERATE_EMBEDDING(
  MODEL `hot-or-not-feed-intelligence.test_yral_video.mm_embed`,
  TABLE `hot-or-not-feed-intelligence.test_yral_video.video_object_table`,
  STRUCT(TRUE AS flatten_json_output,
    10 AS interval_seconds)
)) AS source
on target.uri = source.uri
WHEN MATCHED THEN
  UPDATE SET
    ml_generate_embedding_result = source.ml_generate_embedding_result,
    ml_generate_embedding_status = source.ml_generate_embedding_status,
    ml_generate_embedding_start_sec = source.ml_generate_embedding_start_sec,
    ml_generate_embedding_end_sec = source.ml_generate_embedding_end_sec,
    generation = source.generation,
    content_type = source.content_type,
    size = source.size,
    md5_hash = source.md5_hash,
    updated = source.updated,
    metadata = source.metadata
WHEN NOT MATCHED THEN
  INSERT (
    ml_generate_embedding_result,
    ml_generate_embedding_status,
    ml_generate_embedding_start_sec,
    ml_generate_embedding_end_sec,
    uri,
    generation,
    content_type,
    size,
    md5_hash,
    updated,
    metadata
  )
  VALUES (
    source.ml_generate_embedding_result,
    source.ml_generate_embedding_status,
    source.ml_generate_embedding_start_sec,
    source.ml_generate_embedding_end_sec,
    source.uri,
    source.generation,
    source.content_type,
    source.size,
    source.md5_hash,
    source.updated,
    source.metadata
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

    get_data_object_table = BigQueryGetDataOperator(
        task_id="get_data_from_bq_object_table",
        dataset_id="test_yral_video",
        table_id="video_object_table",
        table_project_id="hot-or-not-feed-intelligence",
        max_results=500000,
        selected_fields="uri",
    )

    get_data_embeddings = BigQueryGetDataOperator(
        task_id="get_data_from_bq_embeddings",
        dataset_id="test_yral_video",
        table_id="video_embeddings",
        table_project_id="hot-or-not-feed-intelligence",
        max_results=1000000,
        selected_fields="uri",
    )

    # Step 2: Process the list to extract object names
    def extract_object_names(**kwargs):
        task_instance = kwargs["task_instance"]
        obj_table_rows = task_instance.xcom_pull(
            task_ids="get_data_from_bq_object_table"
        )
        embedding_rows = task_instance.xcom_pull(task_ids="get_data_from_bq_embeddings")

        obj_table_names = [row[0] for row in obj_table_rows]
        embedding_names = set([row[0] for row in embedding_rows])

        print(obj_table_names[0], len(obj_table_names))
        print(embedding_names[0], len(embedding_names))

        object_names = []
        for uri in obj_table_names:
            if uri in embedding_names:
                # Extract object name from URI
                object_name = uri.split("gs://")[1].split("/", 1)[1]
                object_names.append(object_name)

        # Push the list of object names to XCom
        task_instance.xcom_push(key="object_names", value=object_names)

    process_uris = PythonOperator(
        task_id="process_uris",
        python_callable=extract_object_names,
        provide_context=True,
    )

    def delete_objs(**kwargs):
        hook = GCSHook()
        task_instance = kwargs["task_instance"]
        array_objects = task_instance.xcom_pull(
            task_ids="process_uris", key="object_names"
        )

        array_objects = list(set(array_objects))

        for arr in array_objects:
            hook.delete(bucket_name="yral-videos", object_name=arr)

    delete_gcs_objects = PythonOperator(
        task_id="delete_gcs_obj",
        provide_context=True,
        python_callable=delete_objs,
    )

    # Define task dependencies
    (
        run_create_embed_query
        >> get_data_object_table
        >> get_data_embeddings
        >> process_uris
        >> delete_gcs_objects
    )
