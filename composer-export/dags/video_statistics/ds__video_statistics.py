from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import logging
import requests

from video_statistics.clickhouse_utils import clickhouse_command

logger = logging.getLogger(__name__)

def send_alert_to_google_chat(context=None, text=None):
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG video_statistics_dag failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for video_statistics")

def run_query():
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.user_video_metrics ON CLUSTER yral_cluster
        (
            `user_id` String,
            `user_like_avg` Float32,
            `user_like_stddev` Float32,
            `total_likes` Int64,
            `user_share_avg` Float32,
            `user_share_stddev` Float32,
            `total_shares` Int64,
            `user_watch_percentage_avg` Float32,
            `user_watch_percentage_stddev` Float32,
            `total_watches` Int64,
            `user_normalized_like_avg` Float32,
            `user_normalized_like_stddev` Float32,
            `user_normalized_share_avg` Float32,
            `user_normalized_share_stddev` Float32,
            `user_normalized_watch_percentage_avg` Float32,
            `user_normalized_watch_percentage_stddev` Float32,
            `last_update_timestamp` DateTime64(3),
            `_updated_at` DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/yral/user_video_metrics', '{replica}', _updated_at)
        ORDER BY user_id
        SETTINGS index_granularity = 8192
        """
    )
    clickhouse_command(
        """
        INSERT INTO yral.video_statistics
        (
            video_id,
            user_normalized_like_perc,
            user_normalized_share_perc,
            user_normalized_watch_percentage_perc,
            total_impressions,
            last_update_timestamp,
            _updated_at
        )
        WITH
            ifNull(
                (SELECT max(last_update_timestamp) FROM yral.video_statistics FINAL),
                toDateTime64('1970-01-01 00:00:00', 3)
            ) AS watermark,
            affected_videos AS (
                SELECT DISTINCT video_id
                FROM yral.user_video_relation FINAL
                WHERE last_watched_timestamp >= watermark - INTERVAL 2 HOUR
            ),
            user_contributions AS (
                SELECT
                    uvr.video_id,
                    uvr.user_id,
                    (1 - ifNull(uvm.user_like_avg, 1)) * toUInt8(uvr.liked) AS user_normalized_like_contribution,
                    (1 - ifNull(uvm.user_share_avg, 1)) * toUInt8(uvr.shared) AS user_normalized_share_contribution,
                    (1 - ifNull(uvm.user_watch_percentage_avg, 1)) * uvr.mean_percentage_watched AS user_normalized_watch_contribution,
                    uvr.last_watched_timestamp
                FROM yral.user_video_relation AS uvr FINAL
                LEFT JOIN yral.user_video_metrics AS uvm FINAL
                    ON uvr.user_id = uvm.user_id
                WHERE uvr.video_id IN (SELECT video_id FROM affected_videos)
            )
        SELECT
            video_id,
            avg(user_normalized_like_contribution) AS user_normalized_like_perc,
            avg(user_normalized_share_contribution) AS user_normalized_share_perc,
            avg(user_normalized_watch_contribution) AS user_normalized_watch_percentage_perc,
            count(user_id) AS total_impressions,
            max(last_watched_timestamp) AS last_update_timestamp,
            now64(3) AS _updated_at
        FROM user_contributions
        GROUP BY video_id
        """
    )


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('video_statistics', default_args=default_args, schedule_interval='*/35 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )

