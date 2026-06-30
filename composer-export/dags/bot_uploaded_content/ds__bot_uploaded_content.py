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

def update_bot_uploaded_content():
    try:
        if clickhouse_table_row_count("bot_uploaded_content") == 0:
            raise RuntimeError(
                "ClickHouse bootstrap missing for yral.bot_uploaded_content; "
                "complete bulk load before enabling ClickHouse-only writes"
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
                JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
                JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS publisher_user_id,
                JSON_EXTRACT_SCALAR(params, '$.canister_id') AS canister_id,
                JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
                JSON_EXTRACT_SCALAR(params, '$.post_id') AS post_id,
                JSON_EXTRACT_SCALAR(params, '$.country') AS country,
                JSON_EXTRACT_SCALAR(params, '$.display_name') AS display_name,
                timestamp
            FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
            WHERE event = 'video_upload_successful'
              AND JSON_EXTRACT_SCALAR(params, '$.country') LIKE '%-BOT'
              AND timestamp >= TIMESTAMP_MILLIS(@lower_bound_ms)
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY JSON_EXTRACT_SCALAR(params, '$.video_id')
              ORDER BY timestamp DESC
            ) = 1
            ORDER BY timestamp, video_id
            """,
            job_config=job_config,
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])
        if not data:
            logger.info("bot_uploaded_content: no new source rows to write")
            return

        inserted = clickhouse_insert(table="bot_uploaded_content", data=data)
        logger.info("bot_uploaded_content: wrote %s rows directly to ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse write failed for bot_uploaded_content")
        send_alert_to_google_chat(text="ClickHouse write failed for ds__bot_uploaded_content.")
        raise


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

