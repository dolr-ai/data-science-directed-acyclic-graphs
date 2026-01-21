"""
Airflow DAG to maintain the excluded_videos table.

This DAG identifies videos that should be excluded from recommendation feeds:
    1. REPORTED + NOT CLEAN: Video was reported AND (probability >= 0.4 OR nsfw_ec != 'neutral')
    2. BANNED: Video has a video_report_banned event (direct exclusion, no NSFW check)

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
    message = {"text": "DAG ds__reported_nsfw_videos (excluded_videos table) failed."}
    requests.post(webhook_url, json=message)


def check_table_exists():
    """
    Checks if excluded_videos table exists in BigQuery.

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
    WHERE table_name = 'excluded_videos'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0


def create_initial_query():
    """
    Creates initial SQL to build excluded_videos table from scratch.

    Algorithm:
        1. Source 1 - Reported + NSFW videos:
           a. Parse mp_video_reported events from analytics to get reported video_ids
           b. Join with video_nsfw_agg to get NSFW scores
           c. Filter for NOT CLEAN: probability >= 0.4 OR nsfw_ec != 'neutral'
           d. Deduplicate by video_id (keep earliest report timestamp)
        2. Source 2 - Banned videos:
           a. Parse video_report_banned events from analytics
           b. Direct inclusion without NSFW check
        3. Combine both sources with UNION DISTINCT
        4. Partition by DATE(excluded_at) for efficient queries
        5. Cluster by video_id for fast lookups

    Returns:
        str: SQL query for initial table creation

    Output schema:
        - video_id: STRING - The excluded video
        - excluded_at: TIMESTAMP - When exclusion event occurred
        - exclusion_reason: STRING - 'reported_nsfw' or 'banned'
    """
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.excluded_videos`
PARTITION BY DATE(excluded_at)
CLUSTER BY video_id
AS (
    -- Source 1: Reported + NSFW videos
    WITH reported_nsfw AS (
        SELECT
            JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
            MIN(timestamp) as excluded_at,
            'reported_nsfw' as exclusion_reason
        FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
        WHERE event = 'mp_video_reported'
        AND JSON_EXTRACT_SCALAR(params, '$.video_id') IS NOT NULL
        GROUP BY 1
    ),
    reported_nsfw_filtered AS (
        SELECT
            r.video_id,
            r.excluded_at,
            r.exclusion_reason
        FROM reported_nsfw r
        INNER JOIN `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg` nsfw
            ON r.video_id = nsfw.video_id
        WHERE nsfw.probability >= 0.4 OR nsfw.nsfw_ec != 'neutral'
    ),

    -- Source 2: Banned videos (direct exclusion)
    banned AS (
        SELECT
            JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
            MIN(timestamp) as excluded_at,
            'banned' as exclusion_reason
        FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
        WHERE event = 'video_report_banned'
        AND JSON_EXTRACT_SCALAR(params, '$.video_id') IS NOT NULL
        GROUP BY 1
    )

    -- Combine both sources, dedupe by video_id
    SELECT video_id, excluded_at, exclusion_reason
    FROM reported_nsfw_filtered

    UNION DISTINCT

    SELECT video_id, excluded_at, exclusion_reason
    FROM banned
)
"""


def create_incremental_query():
    """
    Creates incremental SQL to merge new excluded videos.

    Algorithm:
        1. Look back 10 minutes for new events (2x the 5-min interval)
        2. Source 1 - Reported + NSFW:
           a. Get new mp_video_reported events
           b. Join with video_nsfw_agg to filter for NOT CLEAN
        3. Source 2 - Banned:
           a. Get new video_report_banned events
        4. Combine sources with UNION DISTINCT
        5. MERGE into target table on video_id
        6. Update if matched (refresh exclusion data)
        7. Insert if not matched

    Returns:
        str: SQL query for incremental updates
    """
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.excluded_videos` AS target
USING (
    -- Look back 10 minutes for new events
    WITH reported_nsfw AS (
        SELECT
            JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
            MIN(timestamp) as excluded_at,
            'reported_nsfw' as exclusion_reason
        FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
        WHERE event = 'mp_video_reported'
        AND JSON_EXTRACT_SCALAR(params, '$.video_id') IS NOT NULL
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
        GROUP BY 1
    ),
    reported_nsfw_filtered AS (
        SELECT r.video_id, r.excluded_at, r.exclusion_reason
        FROM reported_nsfw r
        INNER JOIN `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg` nsfw
            ON r.video_id = nsfw.video_id
        WHERE nsfw.probability >= 0.4 OR nsfw.nsfw_ec != 'neutral'
    ),
    banned AS (
        SELECT
            JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
            MIN(timestamp) as excluded_at,
            'banned' as exclusion_reason
        FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
        WHERE event = 'video_report_banned'
        AND JSON_EXTRACT_SCALAR(params, '$.video_id') IS NOT NULL
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
        GROUP BY 1
    )
    SELECT * FROM reported_nsfw_filtered
    UNION DISTINCT
    SELECT * FROM banned
) AS source
ON target.video_id = source.video_id
WHEN MATCHED THEN
    UPDATE SET
        target.exclusion_reason = source.exclusion_reason,
        target.excluded_at = source.excluded_at
WHEN NOT MATCHED THEN
    INSERT (video_id, excluded_at, exclusion_reason)
    VALUES (source.video_id, source.excluded_at, source.exclusion_reason)
"""


def update_excluded_videos():
    """
    Main function to update excluded_videos table.

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
    description='Maintains table of excluded videos: reported+NSFW (probability >= 0.4 OR nsfw_ec != neutral) OR banned',
    schedule_interval='*/5 * * * *',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    run_query_task = PythonOperator(
        task_id='update_excluded_videos',
        python_callable=update_excluded_videos,
        on_failure_callback=send_alert_to_google_chat
    )


if __name__ == "__main__":
    update_excluded_videos()
