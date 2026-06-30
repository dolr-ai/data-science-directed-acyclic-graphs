from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests

from global_popular_videos_l90d.clickhouse_utils import clickhouse_command

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG global_popular_videos_l90d failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.global_popular_videos_l90d ON CLUSTER yral_cluster
        (
            `video_id` String,
            `normalized_like_perc` Float32,
            `normalized_watch_perc` Float32,
            `global_popularity_score` Float32
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/yral/global_popular_videos_l90d', '{replica}')
        ORDER BY video_id
        SETTINGS index_granularity = 8192
        """
    )


def create_global_popular_videos_l90d():
    ensure_clickhouse_table()
    clickhouse_command("TRUNCATE TABLE yral.global_popular_videos_l90d")
    clickhouse_command(
        """
        INSERT INTO yral.global_popular_videos_l90d
        (
            video_id,
            normalized_like_perc,
            normalized_watch_perc,
            global_popularity_score
        )
        WITH
            stats AS (
                SELECT
                    video_id,
                    avg(toUInt8(liked)) AS like_perc,
                    avg(mean_percentage_watched) AS watch_perc
                FROM yral.user_video_relation FINAL
                WHERE last_watched_timestamp >= now64(3) - INTERVAL 90 DAY
                GROUP BY video_id
            ),
            stats_with_mean_std AS (
                SELECT
                    video_id,
                    like_perc,
                    watch_perc,
                    avg(like_perc) OVER () AS mean_like_perc,
                    stddevSamp(like_perc) OVER () AS stddev_like_perc,
                    avg(watch_perc) OVER () AS mean_watch_perc,
                    stddevSamp(watch_perc) OVER () AS stddev_watch_perc
                FROM stats
            ),
            normalized_stats AS (
                SELECT
                    video_id,
                    if(stddev_like_perc = 0 OR isNaN(stddev_like_perc), 0, (like_perc - mean_like_perc) / stddev_like_perc) AS normalized_like_perc,
                    if(stddev_watch_perc = 0 OR isNaN(stddev_watch_perc), 0, (watch_perc - mean_watch_perc) / stddev_watch_perc) AS normalized_watch_perc
                FROM stats_with_mean_std
            ),
            offset_stats AS (
                SELECT
                    video_id,
                    normalized_like_perc,
                    normalized_watch_perc,
                    least(normalized_like_perc, normalized_watch_perc) AS min_normalized_perc
                FROM normalized_stats
            )
        SELECT
            video_id,
            normalized_like_perc,
            normalized_watch_perc,
            2 / (1 / (normalized_like_perc - min_normalized_perc + 1 + 1e-9) + 1 / (normalized_watch_perc - min_normalized_perc + 1 + 1e-9)) AS global_popularity_score
        FROM offset_stats
        ORDER BY global_popularity_score DESC
        """
    )

with DAG('global_popular_videos_l90d', default_args=default_args, schedule_interval='10 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=create_global_popular_videos_l90d,
        on_failure_callback=send_alert_to_google_chat
    )