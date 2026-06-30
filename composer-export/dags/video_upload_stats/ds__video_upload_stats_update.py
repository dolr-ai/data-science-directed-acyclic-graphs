from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import bigquery
import requests

from video_upload_stats.clickhouse_utils import (
    add_updated_at,
    clickhouse_command,
    clickhouse_insert,
    clickhouse_max_timestamp_ms,
)

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG video_upload_stats_update failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.video_upload_stats ON CLUSTER yral_cluster
        (
            `device_id` Nullable(String),
            `os` Nullable(String),
            `user_id` Nullable(String),
            `btc_balance_e8s` Nullable(String),
            `city` Nullable(String),
            `country` Nullable(String),
            `creator_commision_percentage` Nullable(String),
            `custom_device_id` Nullable(String),
            `device` Nullable(String),
            `distinct_id` Nullable(String),
            `event` Nullable(String),
            `game_type` Nullable(String),
            `ip` Nullable(String),
            `ip_addr` Nullable(String),
            `is_creator` Nullable(String),
            `is_game_enabled` Nullable(String),
            `is_logged_in` Nullable(String),
            `is_nsfw_enabled` Nullable(String),
            `principal` Nullable(String),
            `region` Nullable(String),
            `sats_balance` Nullable(String),
            `user_agent` Nullable(String),
            `video_id` String,
            `visitor_id` Nullable(String),
            `upload_type` Nullable(String),
            `timestamp` DateTime64(3),
            `_updated_at` DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/yral/video_upload_stats', '{replica}', _updated_at)
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (timestamp, video_id)
        SETTINGS index_granularity = 8192
        """
    )


def update_video_upload_stats():
    ensure_clickhouse_table()
    client = bigquery.Client()
    lower_bound_ms = clickhouse_max_timestamp_ms("video_upload_stats", "timestamp")
    if lower_bound_ms is None:
        lower_bound_ms = 0

    overlap_ms = 2 * 60 * 60 * 1000
    lower_bound_ms = max(lower_bound_ms - overlap_ms, 0)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lower_bound_ms", "INT64", lower_bound_ms),
        ]
    )
    rows_iter = client.query(
        """
        SELECT
            JSON_EXTRACT_SCALAR(data, "$.event_data['$device_id']") AS device_id,
            JSON_EXTRACT_SCALAR(data, "$.event_data['$os']") AS os,
            JSON_EXTRACT_SCALAR(data, "$.event_data['$user_id']") AS user_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.btc_balance_e8s') AS btc_balance_e8s,
            JSON_EXTRACT_SCALAR(data, '$.event_data.city') AS city,
            JSON_EXTRACT_SCALAR(data, '$.event_data.country') AS country,
            JSON_EXTRACT_SCALAR(data, '$.event_data.creator_commision_percentage') AS creator_commision_percentage,
            JSON_EXTRACT_SCALAR(data, '$.event_data.custom_device_id') AS custom_device_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.device') AS device,
            JSON_EXTRACT_SCALAR(data, '$.event_data.distinct_id') AS distinct_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.event') AS event,
            JSON_EXTRACT_SCALAR(data, '$.event_data.game_type') AS game_type,
            JSON_EXTRACT_SCALAR(data, '$.event_data.ip') AS ip,
            JSON_EXTRACT_SCALAR(data, '$.event_data.ip_addr') AS ip_addr,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_creator') AS is_creator,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_game_enabled') AS is_game_enabled,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_logged_in') AS is_logged_in,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_nsfw_enabled') AS is_nsfw_enabled,
            JSON_EXTRACT_SCALAR(data, '$.event_data.principal') AS principal,
            JSON_EXTRACT_SCALAR(data, '$.event_data.region') AS region,
            JSON_EXTRACT_SCALAR(data, '$.event_data.sats_balance') AS sats_balance,
            JSON_EXTRACT_SCALAR(data, '$.event_data.user_agent') AS user_agent,
            JSON_EXTRACT_SCALAR(data, '$.event_data.video_id') AS video_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.visitor_id') AS visitor_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.upload_type') AS upload_type,
            PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(data, '$.timestamp')) AS timestamp
        FROM `hot-or-not-feed-intelligence.yral_ds.analytics_events`
        WHERE JSON_EXTRACT_SCALAR(data, '$.event_data.event') = 'video_upload_success'
          AND PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(data, '$.timestamp')) >= TIMESTAMP_MILLIS(@lower_bound_ms)
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY JSON_EXTRACT_SCALAR(data, '$.event_data.video_id'), PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(data, '$.timestamp'))
          ORDER BY publish_time DESC
        ) = 1
        ORDER BY timestamp, video_id
        """,
        job_config=job_config,
    ).result()
    data = add_updated_at([dict(row) for row in rows_iter])
    if not data:
        return
    clickhouse_insert(table="video_upload_stats", data=data)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ds__video_upload_stats_update',
    default_args=default_args,
    description='A DAG to update video upload stats in ClickHouse',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    is_paused_upon_creation=True
) as dag:
    
    run_query_task = PythonOperator(
        task_id='update_video_upload_stats',
        python_callable=update_video_upload_stats,
        on_failure_callback=send_alert_to_google_chat
    )
