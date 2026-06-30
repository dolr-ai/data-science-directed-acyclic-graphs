from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import requests

from global_video_stats.clickhouse_utils import clickhouse_command

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG global_video_stats_dag failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
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


def run_query():
    ensure_clickhouse_table()
    clickhouse_command("TRUNCATE TABLE yral.global_video_stats")
    clickhouse_command(
        """
        INSERT INTO yral.global_video_stats
        (
            global_avg_user_normalized_likes,
            global_stddev_user_normalized_likes,
            global_avg_user_normalized_shares,
            global_stddev_user_normalized_shares,
            global_avg_user_normalized_watch_percentage,
            global_stddev_user_normalized_watch_percentage,
            total_impressions,
            last_update_timestamp,
            _updated_at
        )
        SELECT
            avg(user_normalized_like_perc) AS global_avg_user_normalized_likes,
            if(isNaN(stddevSamp(toFloat64(user_normalized_like_perc))), 0, stddevSamp(toFloat64(user_normalized_like_perc))) AS global_stddev_user_normalized_likes,
            avg(user_normalized_share_perc) AS global_avg_user_normalized_shares,
            if(isNaN(stddevSamp(toFloat64(user_normalized_share_perc))), 0, stddevSamp(toFloat64(user_normalized_share_perc))) AS global_stddev_user_normalized_shares,
            avg(user_normalized_watch_percentage_perc) AS global_avg_user_normalized_watch_percentage,
            if(isNaN(stddevSamp(toFloat64(user_normalized_watch_percentage_perc))), 0, stddevSamp(toFloat64(user_normalized_watch_percentage_perc))) AS global_stddev_user_normalized_watch_percentage,
            sum(total_impressions) AS total_impressions,
            max(last_update_timestamp) AS last_update_timestamp,
            now64(3) AS _updated_at
        FROM yral.video_statistics FINAL
        WHERE total_impressions IS NOT NULL
        """
    )

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('global_video_stats', default_args=default_args, schedule_interval='*/45 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )
