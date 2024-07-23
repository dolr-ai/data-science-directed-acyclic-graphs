from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime
from google.cloud import bigquery
import requests

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def send_alert_to_google_chat(context):
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAeYc0QQ8/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=QGXm3zD8uV_-OwF_HteGny5k41Dwtario7GQahBlCFs"
    message = {
        "text": f"DAG {context['dag'].dag_id} completed successfully"
    }
    requests.post(webhook_url, json=message)


query = """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.global_popular_videos_l7d` AS
WITH stats AS (
    SELECT 
        video_id,
        AVG(CAST(liked AS INT64)) * 100 AS like_perc,
        AVG(mean_percentage_watched) AS watch_perc
    FROM 
        `hot-or-not-feed-intelligence.yral_ds.userVideoRelation`
    WHERE 
        last_watched_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY 
        video_id
),
stats_with_mean_std AS (
    SELECT
        video_id,
        like_perc,
        watch_perc,
        AVG(like_perc) OVER() AS mean_like_perc,
        STDDEV(like_perc) OVER() AS stddev_like_perc,
        AVG(watch_perc) OVER() AS mean_watch_perc,
        STDDEV(watch_perc) OVER() AS stddev_watch_perc
    FROM
        stats
)
SELECT
    video_id,
    (like_perc - mean_like_perc) / stddev_like_perc AS normalized_like_perc,
    (watch_perc - mean_watch_perc) / stddev_watch_perc AS normalized_watch_perc,
    ((like_perc - mean_like_perc) / stddev_like_perc + (watch_perc - mean_watch_perc) / stddev_watch_perc) / 2 AS global_popularity_score
FROM
    stats_with_mean_std
ORDER BY
    global_popularity_score DESC
"""

def create_global_popular_videos_l7d():
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()

with DAG('global_popular_videos_l7d', default_args=default_args, schedule_interval='10 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=create_global_popular_videos_l7d
    )

    send_alert_task = PythonOperator(
        task_id='send_alert_task',
        python_callable=send_alert_to_google_chat,
        provide_context=True
    )

    run_query_task >> send_alert_task