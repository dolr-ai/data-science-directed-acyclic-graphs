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
        "text": f"DAG ds__video_reports failed."
    }
    requests.post(webhook_url, json=message)


def check_table_exists():
    """Checks if the video_reports table exists in BigQuery.

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
    WHERE table_name = 'video_reports'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0


def create_initial_query():
    """Creates the initial SQL query to build video_reports table from scratch.

    Algorithm:
    1. Creates or replaces the video_reports table
    2. Partitions by DATE(report_timestamp) for efficient time-based queries
    3. Clusters by video_id for fast lookups of reports for specific videos
    4. Extracts mp_video_reported events from test_events_analytics
    5. Parses JSON params to get report details (video_id, reporter, reason, etc.)

    Returns:
        str: SQL query string for initial table creation

    Output schema:
        - video_id: STRING - The video being reported
        - publisher_user_id: STRING - User who uploaded the video
        - canister_id: STRING - Canister ID of the video
        - report_reason: STRING - Reason for report (e.g., 'Nudity / Porn')
        - reporter_user_id: STRING - The user who submitted the report
        - reporter_country: STRING - Country of the reporter
        - report_timestamp: TIMESTAMP - When the report was submitted
    """
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.video_reports`
PARTITION BY DATE(report_timestamp)
CLUSTER BY video_id
AS (
    SELECT
        JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
        JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') as publisher_user_id,
        JSON_EXTRACT_SCALAR(params, '$.canister_id') as canister_id,
        JSON_EXTRACT_SCALAR(params, '$.report_reason') as report_reason,
        JSON_EXTRACT_SCALAR(params, '$.user_id') as reporter_user_id,
        JSON_EXTRACT_SCALAR(params, '$.country') as reporter_country,
        timestamp as report_timestamp
    FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
    WHERE event = 'mp_video_reported'
)
"""


def create_incremental_query():
    """Creates the incremental SQL query to merge new video reports into existing table.

    Algorithm:
    1. Looks back 35 minutes from current time to catch any delayed events
    2. Queries test_events_analytics for new mp_video_reported events in that window
    3. Parses JSON params to extract report details
    4. Uses MERGE to upsert records based on composite key (video_id + reporter_user_id + report_timestamp)
    5. Updates existing records when matched (unlikely for reports but handles duplicates)
    6. Inserts new records when not matched

    Returns:
        str: SQL query string for incremental updates
    """
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.video_reports` AS target
USING (
    SELECT
        JSON_EXTRACT_SCALAR(params, '$.video_id') as video_id,
        JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') as publisher_user_id,
        JSON_EXTRACT_SCALAR(params, '$.canister_id') as canister_id,
        JSON_EXTRACT_SCALAR(params, '$.report_reason') as report_reason,
        JSON_EXTRACT_SCALAR(params, '$.user_id') as reporter_user_id,
        JSON_EXTRACT_SCALAR(params, '$.country') as reporter_country,
        timestamp as report_timestamp
    FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
    WHERE event = 'mp_video_reported'
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 MINUTE)
) AS source
ON target.video_id = source.video_id
    AND target.reporter_user_id = source.reporter_user_id
    AND target.report_timestamp = source.report_timestamp
WHEN MATCHED THEN
    UPDATE SET
        target.publisher_user_id = source.publisher_user_id,
        target.canister_id = source.canister_id,
        target.report_reason = source.report_reason,
        target.reporter_country = source.reporter_country
WHEN NOT MATCHED THEN
    INSERT (
        video_id,
        publisher_user_id,
        canister_id,
        report_reason,
        reporter_user_id,
        reporter_country,
        report_timestamp
    )
    VALUES (
        source.video_id,
        source.publisher_user_id,
        source.canister_id,
        source.report_reason,
        source.reporter_user_id,
        source.reporter_country,
        source.report_timestamp
    )
"""


def update_video_reports():
    """Main function to update video_reports table.

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
    'ds__video_reports',
    default_args=default_args,
    description='A DAG to track video reports (mp_video_reported events) - captures when users report videos with reasons',
    schedule_interval='*/30 * * * *',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    run_query_task = PythonOperator(
        task_id='update_video_reports',
        python_callable=update_video_reports,
        on_failure_callback=send_alert_to_google_chat
    )

if __name__ == "__main__":
    # Test execution locally
    update_video_reports()
