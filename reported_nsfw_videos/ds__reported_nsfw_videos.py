"""
Airflow DAG to maintain the reported_nsfw_videos table.

This DAG identifies videos that are:
    - REPORTED (mp_video_reported event)
    - AND NOT CLEAN (probability >= 0.4 OR nsfw_ec != 'neutral')

These videos are excluded from all recommendation feeds.

Schedule: Every 5 minutes (aligned with feed server exclude set sync)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import bigquery
import requests


def send_alert_to_google_chat():
    """
    Sends failure alert to Google Chat webhook.

    Algorithm:
        1. Construct message payload with DAG name
        2. POST to Google Chat webhook URL
    """
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {"text": "DAG ds__reported_nsfw_videos failed."}
    requests.post(webhook_url, json=message)


def check_table_exists():
    """
    Checks if reported_nsfw_videos table exists in BigQuery.

    Algorithm:
        1. Create BigQuery client
        2. Query INFORMATION_SCHEMA for table existence
        3. Return True if exists, False otherwise

    Returns:
        bool: True if table exists
    """
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'reported_nsfw_videos'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0


def create_initial_query():
    """
    Creates initial SQL to build reported_nsfw_videos table from scratch.

    Algorithm:
        1. Parse mp_video_reported events from analytics to get reported video_ids
        2. Join with video_nsfw_agg to get NSFW scores
        3. Filter for NOT CLEAN: probability >= 0.4 OR nsfw_ec != 'neutral'
        4. Deduplicate by video_id (keep earliest report timestamp)
        5. Partition by DATE(report_timestamp) for efficient queries
        6. Cluster by video_id for fast lookups

    Returns:
        str: SQL query for initial table creation

    Output schema:
        - video_id: STRING - The reported + NSFW video
        - report_timestamp: TIMESTAMP - When first reported
        - probability: FLOAT64 - NSFW probability score
        - nsfw_ec: STRING - NSFW explicit content category
    """
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.reported_nsfw_videos`
PARTITION BY DATE(report_timestamp)
CLUSTER BY video_id
AS (
    WITH reported AS (
        SELECT
            JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
            MIN(timestamp) as report_timestamp
        FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
        WHERE event = 'mp_video_reported'
        AND JSON_EXTRACT_SCALAR(params, '$.video_id') IS NOT NULL
        GROUP BY 1
    )
    SELECT DISTINCT
        r.video_id,
        r.report_timestamp,
        nsfw.probability,
        nsfw.nsfw_ec
    FROM reported r
    INNER JOIN `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg` nsfw
        ON r.video_id = nsfw.video_id
    WHERE
        -- NOT CLEAN condition: probability >= 0.4 OR nsfw_ec != 'neutral'
        nsfw.probability >= 0.4 OR nsfw.nsfw_ec != 'neutral'
)
"""


def create_incremental_query():
    """
    Creates incremental SQL to merge new reported + NSFW videos.

    Algorithm:
        1. Look back 10 minutes for new mp_video_reported events (2x the 5-min interval)
        2. Join with video_nsfw_agg to get NSFW scores
        3. Filter for NOT CLEAN videos
        4. MERGE into target table on video_id
        5. Update if matched (refresh NSFW scores)
        6. Insert if not matched

    Returns:
        str: SQL query for incremental updates
    """
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.reported_nsfw_videos` AS target
USING (
    WITH new_reports AS (
        SELECT
            JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
            MIN(timestamp) as report_timestamp
        FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
        WHERE event = 'mp_video_reported'
        AND JSON_EXTRACT_SCALAR(params, '$.video_id') IS NOT NULL
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
        GROUP BY 1
    )
    SELECT DISTINCT
        r.video_id,
        r.report_timestamp,
        nsfw.probability,
        nsfw.nsfw_ec
    FROM new_reports r
    INNER JOIN `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg` nsfw
        ON r.video_id = nsfw.video_id
    WHERE
        nsfw.probability >= 0.4 OR nsfw.nsfw_ec != 'neutral'
) AS source
ON target.video_id = source.video_id
WHEN MATCHED THEN
    UPDATE SET
        target.probability = source.probability,
        target.nsfw_ec = source.nsfw_ec
WHEN NOT MATCHED THEN
    INSERT (video_id, report_timestamp, probability, nsfw_ec)
    VALUES (source.video_id, source.report_timestamp, source.probability, source.nsfw_ec)
"""


def update_reported_nsfw_videos():
    """
    Main function to update reported_nsfw_videos table.

    Algorithm:
        1. Check if table exists
        2. If exists: run incremental query (MERGE new records)
        3. If not exists: run initial query (CREATE table)
        4. Execute and wait for completion
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
    'ds__reported_nsfw_videos',
    default_args=default_args,
    description='Maintains table of reported videos that are also NSFW (probability >= 0.4 OR nsfw_ec != neutral)',
    schedule_interval='*/5 * * * *',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    run_query_task = PythonOperator(
        task_id='update_reported_nsfw_videos',
        python_callable=update_reported_nsfw_videos,
        on_failure_callback=send_alert_to_google_chat
    )


if __name__ == "__main__":
    update_reported_nsfw_videos()
