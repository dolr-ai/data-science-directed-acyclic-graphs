from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import requests

from normalized_video_statistics.clickhouse_utils import clickhouse_command

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG video_statistics_normalized_dag failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.video_statistics_normalized ON CLUSTER yral_cluster
        (
            `video_id` String,
            `like_percentage_un` Float32,
            `share_percentage_un` Float32,
            `watch_percentage_un` Float32,
            `normalized_like_perc` Float32,
            `normalized_share_perc` Float32,
            `normalized_watch_perc` Float32,
            `total_impressions` Int64,
            `last_update_timestamp` DateTime64(3),
            `ds_quality_score` Float32,
            `_updated_at` DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/yral/video_statistics_normalized', '{replica}', _updated_at)
        ORDER BY video_id
        SETTINGS index_granularity = 8192
        """
    )


def run_query():
    ensure_clickhouse_table()
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.global_video_stats ON CLUSTER yral_cluster
        (
            `global_avg_user_normalized_likes` Float32,
            `global_stddev_user_normalized_likes` Float32,
            `global_avg_user_normalized_shares` Float32,
            `global_stddev_user_normalized_shares` Float32,
            `global_avg_user_normalized_watch_percentage` Float32,
            `global_stddev_user_normalized_watch_percentage` Float32,
            `total_impressions` Int64,
            `last_update_timestamp` DateTime64(3),
            `_updated_at` DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/yral/global_video_stats', '{replica}', _updated_at)
        ORDER BY tuple()
        SETTINGS index_granularity = 8192
        """
    )
    clickhouse_command("TRUNCATE TABLE yral.video_statistics_normalized")
    clickhouse_command(
        """
        INSERT INTO yral.video_statistics_normalized
        (
            video_id,
            like_percentage_un,
            share_percentage_un,
            watch_percentage_un,
            normalized_like_perc,
            normalized_share_perc,
            normalized_watch_perc,
            total_impressions,
            last_update_timestamp,
            ds_quality_score,
            _updated_at
        )
        WITH
            global_stats AS (
                SELECT
                    ifNull(global_avg_user_normalized_likes, 0) AS global_avg_user_normalized_likes,
                    greatest(ifNull(global_stddev_user_normalized_likes, 0.01), 0.01) AS global_stddev_user_normalized_likes,
                    ifNull(global_avg_user_normalized_shares, 0) AS global_avg_user_normalized_shares,
                    greatest(ifNull(global_stddev_user_normalized_shares, 0.01), 0.01) AS global_stddev_user_normalized_shares,
                    ifNull(global_avg_user_normalized_watch_percentage, 0) AS global_avg_user_normalized_watch_percentage,
                    greatest(ifNull(global_stddev_user_normalized_watch_percentage, 0.01), 0.01) AS global_stddev_user_normalized_watch_percentage
                FROM yral.global_video_stats FINAL
                LIMIT 1
            ),
            normalized_stats AS (
                SELECT
                    vs.video_id,
                    vs.user_normalized_like_perc AS like_percentage_un,
                    vs.user_normalized_share_perc AS share_percentage_un,
                    vs.user_normalized_watch_percentage_perc AS watch_percentage_un,
                    ifNull((vs.user_normalized_like_perc - gs.global_avg_user_normalized_likes) / nullIf(gs.global_stddev_user_normalized_likes, 0), 0) AS normalized_like_perc,
                    ifNull((vs.user_normalized_share_perc - gs.global_avg_user_normalized_shares) / nullIf(gs.global_stddev_user_normalized_shares, 0), 0) AS normalized_share_perc,
                    ifNull((vs.user_normalized_watch_percentage_perc - gs.global_avg_user_normalized_watch_percentage) / nullIf(gs.global_stddev_user_normalized_watch_percentage, 0), 0) AS normalized_watch_perc,
                    vs.total_impressions,
                    vs.last_update_timestamp
                FROM yral.video_statistics AS vs FINAL
                CROSS JOIN global_stats AS gs
            )
        SELECT
            video_id,
            like_percentage_un,
            share_percentage_un,
            watch_percentage_un,
            normalized_like_perc,
            normalized_share_perc,
            normalized_watch_perc,
            total_impressions,
            last_update_timestamp,
            3 * (normalized_like_perc + 120) * (normalized_share_perc + 120) * (normalized_watch_perc + 120) /
                (normalized_like_perc + 120 + normalized_share_perc + 120 + normalized_watch_perc + 120) AS ds_quality_score,
            now64(3) AS _updated_at
        FROM normalized_stats
        """
    )

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('video_statistics_normalized', default_args=default_args, schedule_interval='*/50 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )
