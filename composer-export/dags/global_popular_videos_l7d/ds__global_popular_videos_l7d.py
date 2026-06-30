from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import logging
import requests

from global_popular_videos_l7d.clickhouse_utils import get_clickhouse_client

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def send_alert_to_google_chat(context=None, text=None):
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG global_popular_videos_l7d failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for global_popular_videos_l7d")


def create_global_popular_videos_l7d():
    ch_client = get_clickhouse_client()
    ch_client.command(
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
    ch_client.command("TRUNCATE TABLE yral.global_popular_videos_l7d")
    ch_client.command(
        """
        INSERT INTO yral.global_popular_videos_l7d
        (
            video_id,
            normalized_like_perc_p,
            normalized_watch_perc_p,
            global_popularity_score,
            is_nsfw,
            nsfw_ec,
            nsfw_gore,
            nsfw_probability,
            upload_type,
            is_bot_uploaded,
            user_uploaded_ai_content
        )
        WITH
            stats AS (
                SELECT
                    video_id,
                    avg(toUInt8(liked)) AS like_perc,
                    avg(mean_percentage_watched) AS watch_perc,
                    count() AS total_interactions
                FROM yral.user_video_relation FINAL
                WHERE last_watched_timestamp >= now64(3) - INTERVAL 7 DAY
                GROUP BY video_id
            ),
            stats_with_mean_std AS (
                SELECT
                    video_id,
                    like_perc,
                    watch_perc,
                    total_interactions,
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
            ),
            popular_videos AS (
                SELECT
                    video_id,
                    normalized_like_perc - min_normalized_perc + 1 AS normalized_like_perc_p,
                    normalized_watch_perc - min_normalized_perc + 1 AS normalized_watch_perc_p,
                    2 / (1 / (normalized_like_perc - min_normalized_perc + 1 + 1e-9) + 1 / (normalized_watch_perc - min_normalized_perc + 1 + 1e-9)) AS global_popularity_score
                FROM offset_stats
            ),
            upload_stats AS (
                SELECT
                    video_id,
                    argMax(upload_type, timestamp) AS upload_type
                FROM yral.video_upload_stats FINAL
                GROUP BY video_id
            ),
            bot_content AS (
                SELECT DISTINCT video_id
                FROM yral.bot_uploaded_content FINAL
            ),
            approved_ai_content AS (
                SELECT DISTINCT video_id
                FROM yral.ugc_content_approval FINAL
                WHERE is_approved = TRUE
            )
        SELECT
            popular_videos.video_id,
            normalized_like_perc_p,
            normalized_watch_perc_p,
            global_popularity_score,
            toBool(video_nsfw.is_nsfw) AS is_nsfw,
            ifNull(video_nsfw.nsfw_ec, '') AS nsfw_ec,
            ifNull(video_nsfw.nsfw_gore, '') AS nsfw_gore,
            video_nsfw.probability AS nsfw_probability,
            upload_stats.upload_type,
            bot_content.video_id IS NOT NULL AS is_bot_uploaded,
            approved_ai_content.video_id IS NOT NULL AS user_uploaded_ai_content
        FROM popular_videos
        INNER JOIN yral.video_nsfw_agg AS video_nsfw FINAL
            ON popular_videos.video_id = video_nsfw.video_id
        LEFT JOIN upload_stats
            ON popular_videos.video_id = upload_stats.video_id
        LEFT JOIN bot_content
            ON popular_videos.video_id = bot_content.video_id
        LEFT JOIN approved_ai_content
            ON popular_videos.video_id = approved_ai_content.video_id
        ORDER BY global_popularity_score DESC
        """
    )

with DAG('global_popular_videos_l7d', default_args=default_args, schedule_interval='10 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=create_global_popular_videos_l7d,
        on_failure_callback=send_alert_to_google_chat
    )
