from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import logging
import requests

from user_video_relation.clickhouse_utils import (
    add_updated_at,
    clickhouse_insert,
    clickhouse_max_timestamp_ms,
    clickhouse_table_row_count,
)

logger = logging.getLogger(__name__)

def send_alert_to_google_chat(context=None, text=None):
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG user_video_relation failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for user_video_relation")

def run_query():
    try:
        if clickhouse_table_row_count("user_video_relation") == 0:
            raise RuntimeError(
                "ClickHouse bootstrap missing for yral.user_video_relation; "
                "complete bulk load before enabling ClickHouse-only writes"
            )

        client = bigquery.Client()
        lower_bound_ms = clickhouse_max_timestamp_ms("user_video_relation", "last_watched_timestamp")
        if lower_bound_ms is None:
            raise RuntimeError("user_video_relation ClickHouse watermark is NULL; bootstrap state is invalid")

        overlap_ms = 2 * 60 * 60 * 1000
        lower_bound_ms = max(lower_bound_ms - overlap_ms, 0)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lower_bound_ms", "INT64", lower_bound_ms),
            ]
        )
        rows_iter = client.query(
            """
            WITH video_watched AS (
              SELECT
                JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
                JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
                MAX(timestamp) AS last_watched_timestamp,
                AVG(CAST(JSON_EXTRACT_SCALAR(params, '$.percentage_watched') AS FLOAT64)) / 100 AS mean_percentage_watched
              FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
              WHERE event = 'video_duration_watched'
                AND timestamp >= TIMESTAMP_MILLIS(@lower_bound_ms)
                AND CAST(JSON_EXTRACT_SCALAR(params, '$.percentage_watched') AS FLOAT64) <= 100
              GROUP BY user_id, video_id
            ),
            video_liked AS (
              SELECT
                JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
                JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
                MAX(timestamp) AS last_liked_timestamp,
                TRUE AS liked
              FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
              WHERE event = 'like_video'
                AND STRUCT(
                  JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
                  JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id
                ) IN (SELECT AS STRUCT user_id, video_id FROM video_watched)
              GROUP BY user_id, video_id
            ),
            video_shared AS (
              SELECT
                JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
                JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
                MAX(timestamp) AS last_shared_timestamp,
                TRUE AS shared
              FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
              WHERE event = 'share_video'
                AND STRUCT(
                  JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
                  JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id
                ) IN (SELECT AS STRUCT user_id, video_id FROM video_watched)
              GROUP BY user_id, video_id
            )
            SELECT
              vw.user_id,
              vw.video_id,
              vw.last_watched_timestamp,
              vw.mean_percentage_watched,
              vl.last_liked_timestamp,
              COALESCE(vl.liked, FALSE) AS liked,
              vs.last_shared_timestamp,
              COALESCE(vs.shared, FALSE) AS shared
            FROM video_watched vw
            LEFT JOIN video_liked vl
              ON vw.user_id = vl.user_id AND vw.video_id = vl.video_id
            LEFT JOIN video_shared vs
              ON vw.user_id = vs.user_id AND vw.video_id = vs.video_id
            ORDER BY vw.last_watched_timestamp, vw.user_id, vw.video_id
            """,
            job_config=job_config,
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])
        if not data:
            logger.info("user_video_relation: no new source rows to write")
            return

        inserted = clickhouse_insert(table="user_video_relation", data=data)
        logger.info("user_video_relation: wrote %s rows directly to ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse write failed for user_video_relation")
        send_alert_to_google_chat(text="ClickHouse write failed for user_video_relation.")
        raise


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('user_video_interaction_dag', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )

