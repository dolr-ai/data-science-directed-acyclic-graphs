from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.utils.dates import days_ago
import requests

default_args = {
    "owner": "airflow",
    "retries": 0,
    # "depends_on_past": True,
}


def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAeYc0QQ8/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=QGXm3zD8uV_-OwF_HteGny5k41Dwtario7GQahBlCFs"
    message = {
        "text": f"DAG video_embedding_dag failed."
    }
    requests.post(webhook_url, json=message)

"""
One time nsfw initialisation query 

```

MERGE INTO `hot-or-not-feed-intelligence.yral_ds.video_embeddings` AS target
USING (
  SELECT 
    gcs_video_id,
    nsfw_ec,
    nsfw_gore,
    is_nsfw,
    ROW_NUMBER() OVER(PARTITION BY gcs_video_id ORDER BY nsfw_ec DESC) as rn
  FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw`
) AS source
ON target.uri = source.gcs_video_id AND source.rn = 1
WHEN MATCHED THEN
  UPDATE SET 
    target.nsfw_ec = source.nsfw_ec,
    target.nsfw_gore = source.nsfw_gore,
    target.is_nsfw = source.is_nsfw
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET 
    target.nsfw_ec = "UNKNOWN",
    target.nsfw_gore = "UNKNOWN",
    target.is_nsfw = True;   
    
```



One time video index initialisation query 

```

MERGE INTO `hot-or-not-feed-intelligence.yral_ds.video_index` AS target
USING (
  SELECT 
    gcs_video_id,
    nsfw_ec,
    nsfw_gore,
    is_nsfw,
    ROW_NUMBER() OVER(PARTITION BY gcs_video_id ORDER BY nsfw_ec DESC) as rn
  FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw`
) AS source
ON target.uri = source.gcs_video_id AND source.rn = 1
WHEN MATCHED THEN
  UPDATE SET 
    target.nsfw_ec = source.nsfw_ec,
    target.nsfw_gore = source.nsfw_gore,
    target.is_nsfw = source.is_nsfw
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET 
    target.nsfw_ec = "",
    target.nsfw_gore = "",
    target.is_nsfw = FALSE;
    
```

Video Index recreation query 

```

CREATE OR REPLACE VECTOR INDEX vector_video_index
ON `hot-or-not-feed-intelligence.yral_ds.video_index`(embedding)
STORING (uri, post_id, timestamp, canister_id, is_nsfw, nsfw_ec, nsfw_gore)
OPTIONS (
  index_type = 'IVF',
  distance_type = 'cosine'
);

``` 

```

SELECT table_name, index_name, ddl, coverage_percentage
FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.VECTOR_INDEXES`;

```

"""

create_embed_query = """
INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_embeddings` 
WITH embedding_table AS (
  SELECT
    *
  FROM
    ML.GENERATE_EMBEDDING(
      MODEL `hot-or-not-feed-intelligence.yral_ds.mm_embed`,
      (
        SELECT
          *
        FROM
          `hot-or-not-feed-intelligence.yral_ds.video_object_table`
        WHERE
          uri IN ( SELECT gcs_video_id FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw` )
          AND uri NOT IN ( SELECT uri FROM `hot-or-not-feed-intelligence.yral_ds.video_embeddings` )
        LIMIT
          10000
      ),
      STRUCT(TRUE AS flatten_json_output, 10 AS interval_seconds)
    )
)

SELECT
  embedding_table.*,
  is_nsfw,
  nsfw_ec,
  nsfw_gore
FROM
  embedding_table
INNER JOIN
  `hot-or-not-feed-intelligence.yral_ds.video_nsfw` AS video_nsfw
ON
  embedding_table.uri = video_nsfw.gcs_video_id
"""

incremental_update_query = """
MERGE INTO `hot-or-not-feed-intelligence.yral_ds.video_index` AS vi
USING (
  SELECT
    uri,
    (SELECT value FROM UNNEST(metadata) WHERE name = 'post_id') AS post_id,
    (SELECT value FROM UNNEST(metadata) WHERE name = 'timestamp') AS timestamp,
    (SELECT value FROM UNNEST(metadata) WHERE name = 'canister_id') AS canister_id,
    nsfw_ec, 
    nsfw_gore,
    is_nsfw,
    ml_generate_embedding_result as embedding
  FROM
    `hot-or-not-feed-intelligence.yral_ds.video_embeddings`
  WHERE
    ARRAY_LENGTH(ml_generate_embedding_result) = 1408
    AND EXISTS (SELECT 1 FROM UNNEST(metadata) AS metadata_item WHERE metadata_item.name = 'timestamp') -- we want  timestamp, post_id, canister_id, nsfw_ec, nsfw_gore and is_nsfw to be present in the metadata
    AND EXISTS (SELECT 1 FROM UNNEST(metadata) AS metadata_item WHERE metadata_item.name = 'post_id')
    AND EXISTS (SELECT 1 FROM UNNEST(metadata) AS metadata_item WHERE metadata_item.name = 'canister_id')
) AS ve
ON vi.uri = ve.uri
WHEN NOT MATCHED THEN
  INSERT (uri, post_id, timestamp, canister_id, embedding, nsfw_ec, nsfw_gore, is_nsfw)
  VALUES (ve.uri, ve.post_id, ve.timestamp, ve.canister_id, ve.embedding, ve.nsfw_ec, ve.nsfw_gore, ve.is_nsfw)
"""

with DAG(
    "video_embed_pipeline_dag",
    default_args=default_args,
    description="DAG for video embedding pipeline. Runs every hour",
    schedule_interval="15 * * * *",
    max_active_runs=1,  # Ensures only one active run at a time
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def run_create_embed_query(**kwargs):
        hook = BigQueryHook(use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(create_embed_query)

    def run_vector_index_update_query(**kwargs):
        hook = BigQueryHook(use_legacy_sql=False)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(incremental_update_query)

    run_query = PythonOperator(
        task_id="run_create_embed_query",
        provide_context=True,
        python_callable=run_create_embed_query,
        on_failure_callback=send_alert_to_google_chat
    )

    run_update_query = PythonOperator(
        task_id="run_vector_index_update_query",
        provide_context=True,
        python_callable=run_vector_index_update_query,
        on_failure_callback=send_alert_to_google_chat
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
        run_query >> run_update_query
        # >> fetch_data
        # >> process_uris
        # >> transfer_and_delete_gcs_objects
    )
