from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import bigquery
import logging
import requests

from ai_ugc.clickhouse_utils import (
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
        "text": text or "DAG ds__ai_ugc failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for ds__ai_ugc")

def update_ai_ugc():
    try:
        if clickhouse_table_row_count("ai_ugc") == 0:
            raise RuntimeError(
                "ClickHouse bootstrap missing for yral.ai_ugc; "
                "complete bulk load before enabling ClickHouse-only writes"
            )

        client = bigquery.Client()
        lower_bound_ms = clickhouse_max_timestamp_ms("ai_ugc", "upload_timestamp")
        if lower_bound_ms is None:
            raise RuntimeError("ai_ugc ClickHouse watermark is NULL; bootstrap state is invalid")

        overlap_ms = 2 * 60 * 60 * 1000
        lower_bound_ms = max(lower_bound_ms - overlap_ms, 0)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lower_bound_ms", "INT64", lower_bound_ms),
            ]
        )
        rows_iter = client.query(
            """
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
                FROM `hot-or-not-feed-intelligence.yral_ds.analytics_events` AS t
                LEFT JOIN UNNEST(JSON_QUERY_ARRAY(t.data, '$.event_data.rows')) AS t0
                WHERE (
                    JSON_QUERY(t.data, '$.event_data.rows') IS NOT NULL
                    OR JSON_QUERY(t.data, '$.event_data.event') IS NOT NULL
                )
                  AND COALESCE(JSON_VALUE(t0, '$.event_data.event'), JSON_VALUE(t.data, '$.event_data.event')) = 'video_upload_success'
                  AND COALESCE(JSON_VALUE(t0, '$.event_data.type_ext'), JSON_VALUE(t.data, '$.event_data.type_ext')) = 'ai_video'
                  AND t.publish_time >= TIMESTAMP_MILLIS(@lower_bound_ms)
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
              AND t1.timestamp >= TIMESTAMP_MILLIS(@lower_bound_ms)
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY ai.video_id
              ORDER BY t1.timestamp DESC, ai.publish_time DESC
            ) = 1
            ORDER BY upload_timestamp, video_id
            """,
            job_config=job_config,
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])
        if not data:
            logger.info("ai_ugc: no new source rows to write")
            return

        inserted = clickhouse_insert(table="ai_ugc", data=data)
        logger.info("ai_ugc: wrote %s rows directly to ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse write failed for ai_ugc")
        send_alert_to_google_chat(text="ClickHouse write failed for ds__ai_ugc.")
        raise


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
