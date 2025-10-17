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
        "text": f"DAG ds__bot_uploaded_content failed."
    }
    requests.post(webhook_url, json=message)

def check_table_exists():
    """Checks if the bot_uploaded_content table exists in BigQuery.

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
    FROM `yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'bot_uploaded_content'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def create_initial_query():
    """Creates the initial SQL query to build bot_uploaded_content table from scratch.

    Algorithm:
    1. Creates or replaces the bot_uploaded_content table
    2. Partitions by DATE(timestamp) for efficient querying
    3. Clusters by video_id for fast lookups and joins
    4. Extracts bot upload events from test_events_analytics
    5. Filters for video_upload_successful events where country ends with '-BOT'
    6. Extracts relevant fields from JSON params

    Returns:
        str: SQL query string for initial table creation
    """
    return """
CREATE OR REPLACE TABLE `yral_ds.bot_uploaded_content`
PARTITION BY DATE(timestamp)
CLUSTER BY video_id
AS (
    SELECT
        JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS publisher_user_id,
        JSON_EXTRACT_SCALAR(params, '$.canister_id') AS canister_id,
        JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
        JSON_EXTRACT_SCALAR(params, '$.post_id') AS post_id,
        JSON_EXTRACT_SCALAR(params, '$.country') AS country,
        JSON_EXTRACT_SCALAR(params, '$.display_name') AS display_name,
        timestamp
    FROM `analytics_335143420.test_events_analytics`
    WHERE event = 'video_upload_successful'
      AND JSON_EXTRACT_SCALAR(params, '$.country') LIKE '%-BOT'
)
"""

def create_incremental_query():
    """Creates the incremental SQL query to merge new bot uploads into existing table.

    Algorithm:
    1. Finds the maximum timestamp from existing table
    2. Queries test_events_analytics for new bot upload events after that timestamp
    3. Uses MERGE to upsert new records based on video_id and timestamp
    4. Inserts only records that don't already exist

    Returns:
        str: SQL query string for incremental updates
    """
    return """
MERGE `yral_ds.bot_uploaded_content` AS target
USING (
    WITH last_ts AS (
        SELECT MAX(timestamp) AS max_ts FROM `yral_ds.bot_uploaded_content`
    )
    SELECT
        JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS publisher_user_id,
        JSON_EXTRACT_SCALAR(params, '$.canister_id') AS canister_id,
        JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
        JSON_EXTRACT_SCALAR(params, '$.post_id') AS post_id,
        JSON_EXTRACT_SCALAR(params, '$.country') AS country,
        JSON_EXTRACT_SCALAR(params, '$.display_name') AS display_name,
        timestamp
    FROM `analytics_335143420.test_events_analytics`, last_ts
    WHERE event = 'video_upload_successful'
      AND JSON_EXTRACT_SCALAR(params, '$.country') LIKE '%-BOT'
      AND timestamp > IFNULL(last_ts.max_ts, TIMESTAMP('1970-01-01'))
) AS source
ON target.video_id = source.video_id AND target.timestamp = source.timestamp
WHEN NOT MATCHED THEN
    INSERT ROW
"""

def update_bot_uploaded_content():
    """Main function to update bot_uploaded_content table.

    Algorithm:
    1. Checks if the table exists using check_table_exists()
    2. If table exists, runs incremental query to add new records
    3. If table doesn't exist, runs initial query to create and populate table
    4. Executes the chosen query using BigQuery client
    5. Waits for query completion
    """
    if check_table_exists():
        query = create_incremental_query()
    else:
        query = create_initial_query()

    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ds__bot_uploaded_content',
    default_args=default_args,
    description='A DAG to track videos uploaded by bots (country ending with -BOT)',
    schedule_interval='0 */3 * * *',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    run_query_task = PythonOperator(
        task_id='update_bot_uploaded_content',
        python_callable=update_bot_uploaded_content,
        on_failure_callback=send_alert_to_google_chat
    )
