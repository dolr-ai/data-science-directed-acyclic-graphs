from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import requests

from user_video_metrics.clickhouse_utils import clickhouse_command

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG user_video_metrics_dag failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
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


def run_query():
    ensure_clickhouse_table()
    clickhouse_command(
        """
        INSERT INTO yral.user_video_metrics
        (
            user_id,
            user_like_avg,
            user_like_stddev,
            total_likes,
            user_share_avg,
            user_share_stddev,
            total_shares,
            user_watch_percentage_avg,
            user_watch_percentage_stddev,
            total_watches,
            user_normalized_like_avg,
            user_normalized_like_stddev,
            user_normalized_share_avg,
            user_normalized_share_stddev,
            user_normalized_watch_percentage_avg,
            user_normalized_watch_percentage_stddev,
            last_update_timestamp,
            _updated_at
        )
        WITH
            ifNull(
                (SELECT max(last_update_timestamp) FROM yral.user_video_metrics FINAL),
                toDateTime64('1970-01-01 00:00:00', 3)
            ) AS watermark,
            affected_users AS (
                SELECT DISTINCT user_id
                FROM yral.user_video_relation FINAL
                WHERE last_watched_timestamp >= watermark - INTERVAL 2 HOUR
            ),
            base_stats AS (
                SELECT
                    user_id,
                    avg(toUInt8(liked)) AS user_like_avg,
                    if(isNaN(stddevSamp(toFloat64(toUInt8(liked)))), 0, stddevSamp(toFloat64(toUInt8(liked)))) AS user_like_stddev,
                    sum(toUInt8(liked)) AS total_likes,
                    avg(toUInt8(shared)) AS user_share_avg,
                    if(isNaN(stddevSamp(toFloat64(toUInt8(shared)))), 0, stddevSamp(toFloat64(toUInt8(shared)))) AS user_share_stddev,
                    sum(toUInt8(shared)) AS total_shares,
                    avg(mean_percentage_watched) AS user_watch_percentage_avg,
                    if(isNaN(stddevSamp(toFloat64(mean_percentage_watched))), 0, stddevSamp(toFloat64(mean_percentage_watched))) AS user_watch_percentage_stddev,
                    count(mean_percentage_watched) AS total_watches,
                    max(last_watched_timestamp) AS last_update_timestamp
                FROM yral.user_video_relation FINAL
                WHERE user_id IN (SELECT user_id FROM affected_users)
                GROUP BY user_id
            )
        SELECT
            user_id,
            user_like_avg,
            user_like_stddev,
            total_likes,
            user_share_avg,
            user_share_stddev,
            total_shares,
            user_watch_percentage_avg,
            user_watch_percentage_stddev,
            total_watches,
            user_like_avg * (1 - user_like_avg) AS user_normalized_like_avg,
            (1 - user_like_avg) * user_like_stddev AS user_normalized_like_stddev,
            user_share_avg * (1 - user_share_avg) AS user_normalized_share_avg,
            (1 - user_share_avg) * user_share_stddev AS user_normalized_share_stddev,
            user_watch_percentage_avg * (1 - user_watch_percentage_avg) AS user_normalized_watch_percentage_avg,
            (1 - user_watch_percentage_avg) * user_watch_percentage_stddev AS user_normalized_watch_percentage_stddev,
            last_update_timestamp,
            now64(3) AS _updated_at
        FROM base_stats
        """
    )

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('user_video_metrics', default_args=default_args, schedule_interval='*/30 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )
