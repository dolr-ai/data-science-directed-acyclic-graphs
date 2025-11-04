from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import bigquery
import requests

def send_alert_to_google_chat():
    """Sends failure alert to Google Chat webhook.

    Algorithm:
    1. Defines the webhook URL for Google Chat notifications
    2. Creates a message payload with DAG failure information
    3. Posts the message to the webhook
    """
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG ds__ai_ugc failed."
    }
    requests.post(webhook_url, json=message)

def check_table_exists():
    """Checks if the ai_ugc table exists in BigQuery.

    Algorithm:
    1. Creates a BigQuery client
    2. Queries INFORMATION_SCHEMA to check for table existence
    3. Returns True if table exists, False otherwise

    Returns:
        bool: True if table exists, False otherwise
    """
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'ai_ugc'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def create_initial_query():
    """Creates the initial SQL query to build ai_ugc table from scratch.

    Algorithm:
    1. Creates or replaces the ai_ugc table
    2. Partitions by DATE(upload_timestamp) for efficient querying
    3. Clusters by video_id for fast lookups and joins
    4. Extracts AI video upload events from analytics_events
    5. Joins with test_events_analytics to get post_id and other metadata
    6. Filters for video_upload_success events with type_ext='ai_video'

    Returns:
        str: SQL query string for initial table creation
    """
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.ai_ugc`
PARTITION BY DATE(upload_timestamp)
CLUSTER BY video_id
AS (
    WITH ai_uploads AS (
        SELECT
            COALESCE(JSON_VALUE(t0, '$.event_data.event'), JSON_VALUE(t.data, '$.event_data.event')) AS event,
            REPLACE(
                COALESCE(JSON_VALUE(t0, '$.event_data.video_id'), JSON_VALUE(t.data, '$.event_data.video_id')),
                '"', ''
            ) AS video_id,
            COALESCE(JSON_VALUE(t0, '$.event_data.publisher_user_id'), JSON_VALUE(t.data, '$.event_data.publisher_user_id')) AS publisher_user_id,
            COALESCE(JSON_VALUE(t0, '$.event_data.user_id'), JSON_VALUE(t.data, '$.event_data.user_id')) AS user_id,
            COALESCE(JSON_VALUE(t0, '$.event_data.canister_id'), JSON_VALUE(t.data, '$.event_data.canister_id')) AS ai_canister_id,
            COALESCE(JSON_VALUE(t0, '$.timestamp'), JSON_VALUE(t.data, '$.timestamp')) AS event_timestamp_str,
            t.publish_time,
            t.publish_time AS received_timestamp,
            COALESCE(JSON_QUERY(t0, '$.event_data'), JSON_QUERY(t.data, '$.event_data')) AS event_json_data,
            COALESCE(JSON_VALUE(t0, '$.event_data.type'), JSON_VALUE(t.data, '$.event_data.type')) AS event_type,
            COALESCE(JSON_VALUE(t0, '$.event_data.type_ext'), JSON_VALUE(t.data, '$.event_data.type_ext')) AS type_ext
        FROM
            `hot-or-not-feed-intelligence.yral_ds.analytics_events` AS t
        LEFT JOIN
            UNNEST(JSON_QUERY_ARRAY(t.data, '$.event_data.rows')) AS t0
        WHERE
            (
                JSON_QUERY(t.data, '$.event_data.rows') IS NOT NULL
                OR JSON_QUERY(t.data, '$.event_data.event') IS NOT NULL
            )
            AND COALESCE(JSON_VALUE(t0, '$.event_data.event'), JSON_VALUE(t.data, '$.event_data.event')) = 'video_upload_success'
            AND COALESCE(JSON_VALUE(t0, '$.event_data.type_ext'), JSON_VALUE(t.data, '$.event_data.type_ext')) = 'ai_video'
    )
    SELECT
        ai.video_id,
        ai.publisher_user_id,
        ai.user_id,
        ai.ai_canister_id,
        ai.event_timestamp_str,
        ai.publish_time,
        ai.received_timestamp,
        ai.event_json_data,
        ai.event_type,
        ai.type_ext,
        t1.timestamp AS upload_timestamp,
        JSON_EXTRACT_SCALAR(t1.params, '$.post_id') AS post_id,
        JSON_EXTRACT_SCALAR(t1.params, '$.canister_id') AS upload_canister_id,
        JSON_EXTRACT_SCALAR(t1.params, '$.country') AS country,
        JSON_EXTRACT_SCALAR(t1.params, '$.display_name') AS display_name
    FROM ai_uploads ai
    INNER JOIN `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics` AS t1
        ON ai.video_id = JSON_EXTRACT_SCALAR(t1.params, '$.video_id')
    WHERE t1.event = 'video_upload_successful'
)
"""

def create_incremental_query():
    """Creates the incremental SQL query to merge new AI uploads into existing table.

    Algorithm:
    1. Looks back 35 minutes from current time to catch any delayed events
    2. Queries analytics_events for new AI video upload events in that window
    3. Joins with test_events_analytics to get complete metadata
    4. Uses MERGE to upsert records based on video_id
    5. Updates existing records with latest data when matched
    6. Inserts new records when not matched

    Returns:
        str: SQL query string for incremental updates
    """
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.ai_ugc` AS target
USING (
    WITH ai_uploads AS (
        SELECT
            COALESCE(JSON_VALUE(t0, '$.event_data.event'), JSON_VALUE(t.data, '$.event_data.event')) AS event,
            REPLACE(
                COALESCE(JSON_VALUE(t0, '$.event_data.video_id'), JSON_VALUE(t.data, '$.event_data.video_id')),
                '"', ''
            ) AS video_id,
            COALESCE(JSON_VALUE(t0, '$.event_data.publisher_user_id'), JSON_VALUE(t.data, '$.event_data.publisher_user_id')) AS publisher_user_id,
            COALESCE(JSON_VALUE(t0, '$.event_data.user_id'), JSON_VALUE(t.data, '$.event_data.user_id')) AS user_id,
            COALESCE(JSON_VALUE(t0, '$.event_data.canister_id'), JSON_VALUE(t.data, '$.event_data.canister_id')) AS ai_canister_id,
            COALESCE(JSON_VALUE(t0, '$.timestamp'), JSON_VALUE(t.data, '$.timestamp')) AS event_timestamp_str,
            t.publish_time,
            t.publish_time AS received_timestamp,
            COALESCE(JSON_QUERY(t0, '$.event_data'), JSON_QUERY(t.data, '$.event_data')) AS event_json_data,
            COALESCE(JSON_VALUE(t0, '$.event_data.type'), JSON_VALUE(t.data, '$.event_data.type')) AS event_type,
            COALESCE(JSON_VALUE(t0, '$.event_data.type_ext'), JSON_VALUE(t.data, '$.event_data.type_ext')) AS type_ext
        FROM
            `hot-or-not-feed-intelligence.yral_ds.analytics_events` AS t
        LEFT JOIN
            UNNEST(JSON_QUERY_ARRAY(t.data, '$.event_data.rows')) AS t0
        WHERE
            (
                JSON_QUERY(t.data, '$.event_data.rows') IS NOT NULL
                OR JSON_QUERY(t.data, '$.event_data.event') IS NOT NULL
            )
            AND COALESCE(JSON_VALUE(t0, '$.event_data.event'), JSON_VALUE(t.data, '$.event_data.event')) = 'video_upload_success'
            AND COALESCE(JSON_VALUE(t0, '$.event_data.type_ext'), JSON_VALUE(t.data, '$.event_data.type_ext')) = 'ai_video'
            AND t.publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 MINUTE)
    )
    SELECT
        ai.video_id,
        ai.publisher_user_id,
        ai.user_id,
        ai.ai_canister_id,
        ai.event_timestamp_str,
        ai.publish_time,
        ai.received_timestamp,
        ai.event_json_data,
        ai.event_type,
        ai.type_ext,
        t1.timestamp AS upload_timestamp,
        JSON_EXTRACT_SCALAR(t1.params, '$.post_id') AS post_id,
        JSON_EXTRACT_SCALAR(t1.params, '$.canister_id') AS upload_canister_id,
        JSON_EXTRACT_SCALAR(t1.params, '$.country') AS country,
        JSON_EXTRACT_SCALAR(t1.params, '$.display_name') AS display_name
    FROM ai_uploads ai
    INNER JOIN `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics` AS t1
        ON ai.video_id = JSON_EXTRACT_SCALAR(t1.params, '$.video_id')
    WHERE t1.event = 'video_upload_successful'
        AND t1.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 MINUTE)
) AS source
ON target.video_id = source.video_id
WHEN MATCHED THEN
    UPDATE SET
        target.publisher_user_id = source.publisher_user_id,
        target.user_id = source.user_id,
        target.ai_canister_id = source.ai_canister_id,
        target.event_timestamp_str = source.event_timestamp_str,
        target.publish_time = source.publish_time,
        target.received_timestamp = source.received_timestamp,
        target.event_json_data = source.event_json_data,
        target.event_type = source.event_type,
        target.type_ext = source.type_ext,
        target.upload_timestamp = source.upload_timestamp,
        target.post_id = source.post_id,
        target.upload_canister_id = source.upload_canister_id,
        target.country = source.country,
        target.display_name = source.display_name
WHEN NOT MATCHED THEN
    INSERT (
        video_id,
        publisher_user_id,
        user_id,
        ai_canister_id,
        event_timestamp_str,
        publish_time,
        received_timestamp,
        event_json_data,
        event_type,
        type_ext,
        upload_timestamp,
        post_id,
        upload_canister_id,
        country,
        display_name
    )
    VALUES (
        source.video_id,
        source.publisher_user_id,
        source.user_id,
        source.ai_canister_id,
        source.event_timestamp_str,
        source.publish_time,
        source.received_timestamp,
        source.event_json_data,
        source.event_type,
        source.type_ext,
        source.upload_timestamp,
        source.post_id,
        source.upload_canister_id,
        source.country,
        source.display_name
    )
"""

def update_ai_ugc():
    """Main function to update ai_ugc table.

    Algorithm:
    1. Checks if the table exists using check_table_exists()
    2. If table exists, runs incremental query to merge new records
    3. If table doesn't exist, runs initial query to create and populate table
    4. Executes the chosen query using BigQuery client
    5. Waits for query completion
    """
    if check_table_exists():
        query = create_incremental_query()
        print("Running incremental update query...")
    else:
        query = create_initial_query()
        print("Running initial table creation query...")

    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()
    print(f"Query completed successfully. Job ID: {query_job.job_id}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ds__ai_ugc',
    default_args=default_args,
    description='A DAG to track AI-generated user content (videos uploaded with type_ext=ai_video)',
    schedule_interval='*/30 * * * *',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    run_query_task = PythonOperator(
        task_id='update_ai_ugc',
        python_callable=update_ai_ugc,
        on_failure_callback=send_alert_to_google_chat
    )

if __name__ == "__main__":
    # Test execution locally
    update_ai_ugc()