from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests

from region_popular_videos_l7d.clickhouse_utils import clickhouse_command

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG region_popular_videos_l7d failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.region_grossing_l7d_candidates ON CLUSTER yral_cluster
        (
            `video_id` String,
            `region` String,
            `normalized_like_perc_p` Float32,
            `normalized_watch_perc_p` Float32,
            `within_region_popularity_score` Float32
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/yral/region_grossing_l7d_candidates', '{replica}')
        ORDER BY (region, video_id)
        SETTINGS index_granularity = 8192
        """
    )


def create_region_popular_videos_l7d():
    ensure_clickhouse_table()
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
    clickhouse_command("TRUNCATE TABLE yral.region_grossing_l7d_candidates")
    clickhouse_command(
        """
        INSERT INTO yral.region_grossing_l7d_candidates
        (
            video_id,
            region,
            normalized_like_perc_p,
            normalized_watch_perc_p,
            within_region_popularity_score
        )
        WITH
            upload_region AS (
                SELECT
                    video_id,
                    argMax(region, timestamp) AS region
                FROM yral.video_upload_stats FINAL
                WHERE region IS NOT NULL
                GROUP BY video_id
            ),
            relevant_interactions_with_location AS (
                SELECT
                    uvr.video_id,
                    uvr.liked,
                    uvr.mean_percentage_watched,
                    upload_region.region
                FROM yral.user_video_relation AS uvr FINAL
                INNER JOIN upload_region
                    ON uvr.video_id = upload_region.video_id
                WHERE uvr.last_watched_timestamp >= now64(3) - INTERVAL 7 DAY
            ),
            stats AS (
                SELECT
                    video_id,
                    region,
                    avg(toUInt8(liked)) AS like_perc,
                    avg(mean_percentage_watched) AS watch_perc
                FROM relevant_interactions_with_location
                GROUP BY video_id, region
            ),
            stats_with_mean_std AS (
                SELECT
                    video_id,
                    region,
                    like_perc,
                    watch_perc,
                    avg(like_perc) OVER () AS mean_like_perc,
                    if(stddevSamp(like_perc) OVER () = 0 OR isNaN(stddevSamp(like_perc) OVER ()), 100, stddevSamp(like_perc) OVER ()) AS stddev_like_perc,
                    avg(watch_perc) OVER () AS mean_watch_perc,
                    if(stddevSamp(watch_perc) OVER () = 0 OR isNaN(stddevSamp(watch_perc) OVER ()), 100, stddevSamp(watch_perc) OVER ()) AS stddev_watch_perc
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
            normalized_like_perc - min_normalized_perc + 1 AS normalized_like_perc_p,
            normalized_watch_perc - min_normalized_perc + 1 AS normalized_watch_perc_p,
            2 / (1 / (normalized_like_perc - min_normalized_perc + 1 + 1e-9) + 1 / (normalized_watch_perc - min_normalized_perc + 1 + 1e-9)) AS within_region_popularity_score
        FROM offset_stats
        ORDER BY region, within_region_popularity_score DESC
        """
    )

with DAG('region_popular_videos_l7d', default_args=default_args, schedule_interval='10 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=create_region_popular_videos_l7d,
        on_failure_callback=send_alert_to_google_chat
    )