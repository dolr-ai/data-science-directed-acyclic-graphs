from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests

from local_popular_videos_l7d.clickhouse_utils import clickhouse_command

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG local_popular_videos_l7d failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.local_popular_videos_l7d ON CLUSTER yral_cluster
        (
            `video_id` String,
            `region` String,
            `normalized_like_perc` Float32,
            `normalized_watch_perc` Float32,
            `local_popularity_score` Float32
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/yral/local_popular_videos_l7d', '{replica}')
        ORDER BY (region, video_id)
        SETTINGS index_granularity = 8192
        """
    )


def create_local_popular_videos_l7d():
    ensure_clickhouse_table()
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.user_base_facts ON CLUSTER yral_cluster
        (
            `user_id` String,
            `region` Nullable(String),
            `occurrence_date` Nullable(Date),
            `last_updated_date` Date,
            `_updated_at` DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/yral/user_base_facts', '{replica}', _updated_at)
        ORDER BY user_id
        SETTINGS index_granularity = 8192
        """
    )
    clickhouse_command("TRUNCATE TABLE yral.local_popular_videos_l7d")
    clickhouse_command(
        """
        INSERT INTO yral.local_popular_videos_l7d
        (
            video_id,
            region,
            normalized_like_perc,
            normalized_watch_perc,
            local_popularity_score
        )
        WITH
            stats AS (
                SELECT
                    uvr.video_id,
                    ifNull(ubf.region, '') AS region,
                    avg(toUInt8(uvr.liked)) AS like_perc,
                    avg(uvr.mean_percentage_watched) AS watch_perc
                FROM yral.user_video_relation AS uvr FINAL
                LEFT JOIN yral.user_base_facts AS ubf FINAL
                    ON uvr.user_id = ubf.user_id
                WHERE uvr.last_watched_timestamp >= now64(3) - INTERVAL 7 DAY
                GROUP BY uvr.video_id, region
            ),
            stats_with_mean_std AS (
                SELECT
                    video_id,
                    region,
                    like_perc,
                    watch_perc,
                    avg(like_perc) OVER (PARTITION BY region) AS mean_like_perc,
                    if(stddevSamp(like_perc) OVER (PARTITION BY region) = 0 OR isNaN(stddevSamp(like_perc) OVER (PARTITION BY region)), 100, stddevSamp(like_perc) OVER (PARTITION BY region)) AS stddev_like_perc,
                    avg(watch_perc) OVER (PARTITION BY region) AS mean_watch_perc,
                    if(stddevSamp(watch_perc) OVER (PARTITION BY region) = 0 OR isNaN(stddevSamp(watch_perc) OVER (PARTITION BY region)), 100, stddevSamp(watch_perc) OVER (PARTITION BY region)) AS stddev_watch_perc
                FROM stats
            ),
            normalized_stats AS (
                SELECT
                    video_id,
                    region,
                    (like_perc - mean_like_perc) / stddev_like_perc AS normalized_like_perc,
                    (watch_perc - mean_watch_perc) / stddev_watch_perc AS normalized_watch_perc
                FROM stats_with_mean_std
            ),
            offset_stats AS (
                SELECT
                    video_id,
                    region,
                    normalized_like_perc,
                    normalized_watch_perc,
                    least(normalized_like_perc, normalized_watch_perc) AS min_normalized_perc
                FROM normalized_stats
            )
        SELECT
            video_id,
            region,
            normalized_like_perc,
            normalized_watch_perc,
            2 / (1 / (normalized_like_perc - min_normalized_perc + 1 + 1e-9) + 1 / (normalized_watch_perc - min_normalized_perc + 1 + 1e-9)) AS local_popularity_score
        FROM offset_stats
        ORDER BY region DESC, local_popularity_score DESC
        """
    )

with DAG('local_popular_videos_l7d', default_args=default_args, schedule_interval='10 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=create_local_popular_videos_l7d,
        on_failure_callback=send_alert_to_google_chat
    )