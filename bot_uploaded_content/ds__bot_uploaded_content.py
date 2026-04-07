from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import bigquery
import logging
import requests

from bot_uploaded_content.clickhouse_utils import (
    add_updated_at,
    clickhouse_insert,
    clickhouse_max_timestamp_ms,
    clickhouse_table_row_count,
)

logger = logging.getLogger(__name__)

def send_alert_to_google_chat(context=None, text=None):
    """Sends failure alert to Google Chat webhook.

    Algorithm:
    1. Defines the webhook URL for Google Chat notifications
    2. Creates a message payload with DAG failure information
    3. Posts the message to the webhook
    """
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG ds__bot_uploaded_content failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for ds__bot_uploaded_content")

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


def sync_to_clickhouse():
    """Sync recently written bot-uploaded rows from BigQuery to ClickHouse."""
    try:
        if clickhouse_table_row_count("bot_uploaded_content") == 0:
            raise RuntimeError(
                "ClickHouse bootstrap missing for yral.bot_uploaded_content; "
                "complete Phase 2 bulk load before enabling Phase 3 dual-write"
            )

        client = bigquery.Client()
        lower_bound_ms = clickhouse_max_timestamp_ms("bot_uploaded_content", "timestamp")
        if lower_bound_ms is None:
            raise RuntimeError("bot_uploaded_content ClickHouse watermark is NULL; bootstrap state is invalid")

        overlap_ms = 6 * 60 * 60 * 1000
        lower_bound_ms = max(lower_bound_ms - overlap_ms, 0)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lower_bound_ms", "INT64", lower_bound_ms),
            ]
        )
        rows_iter = client.query(
            """
            SELECT
                user_id,
                publisher_user_id,
                canister_id,
                video_id,
                post_id,
                country,
                display_name,
                timestamp
            FROM `hot-or-not-feed-intelligence.yral_ds.bot_uploaded_content`
            WHERE timestamp >= TIMESTAMP_MILLIS(@lower_bound_ms)
            ORDER BY timestamp, video_id
            """,
            job_config=job_config,
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])
        if not data:
            logger.info("bot_uploaded_content: no recent rows to sync")
            return

        inserted = clickhouse_insert(table="bot_uploaded_content", data=data)
        logger.info("bot_uploaded_content: synced %s rows to ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse sync failed for bot_uploaded_content")
        send_alert_to_google_chat(
            text=(
                "ClickHouse sync failed for ds__bot_uploaded_content, but the BigQuery write "
                "already completed. The DAG run will continue and ClickHouse catch-up is required."
            )
        )
        return

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

    sync_ch_task = PythonOperator(
        task_id='sync_to_clickhouse',
        python_callable=sync_to_clickhouse,
    )

    run_query_task >> sync_ch_task
