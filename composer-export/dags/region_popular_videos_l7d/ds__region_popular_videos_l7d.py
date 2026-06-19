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

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG region_popular_videos_l7d failed."
    }
    requests.post(webhook_url, json=message)

query = """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.region_grossing_l7d_candidates` AS

WITH relevant_interactions as 
(
select *  
    FROM 
        `hot-or-not-feed-intelligence.yral_ds.userVideoRelation`
    WHERE 
        last_watched_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
),

relevant_interactions_with_location AS (
    SELECT 
        relevant_interactions.*, 
        video_upload_stats.region
    FROM relevant_interactions 
    INNER JOIN `hot-or-not-feed-intelligence.yral_ds.video_upload_stats` AS video_upload_stats
        ON relevant_interactions.video_id = video_upload_stats.video_id
        where region is not null
),

stats AS (
    SELECT 
        video_id,
        region,
        AVG(CAST(liked AS INT64)) AS like_perc,
        AVG(mean_percentage_watched) AS watch_perc
    FROM relevant_interactions_with_location
    GROUP BY 
        video_id, region
),

stats_with_mean_std AS (
    SELECT
        video_id,
        region,
        like_perc,
        watch_perc,
        AVG(like_perc) OVER() AS mean_like_perc, -- global mean like_perc
        COALESCE(NULLIF(STDDEV(like_perc) OVER(), 0), 100) AS stddev_like_perc, -- global stddev like_perc
        AVG(watch_perc) OVER() AS mean_watch_perc, -- global mean watch_perc
        COALESCE(NULLIF(STDDEV(watch_perc) OVER(), 0), 100) AS stddev_watch_perc -- global stddev watch_perc
    FROM
        stats
),
normalized_stats AS (
    SELECT
        video_id,
        region,
        (like_perc - mean_like_perc) / stddev_like_perc AS normalized_like_perc,
        (watch_perc - mean_watch_perc) / stddev_watch_perc AS normalized_watch_perc
    FROM
        stats_with_mean_std
),
offset_stats AS (
    SELECT
        video_id,
        region,
        normalized_like_perc,
        normalized_watch_perc,
        LEAST(normalized_like_perc, normalized_watch_perc) AS min_normalized_perc
    FROM
        normalized_stats
), 
region_popular_videos AS (
    SELECT
        video_id,
        region,
        normalized_like_perc - min_normalized_perc + 1 as normalized_like_perc_p,
        normalized_watch_perc - min_normalized_perc + 1 as normalized_watch_perc_p,
        2 / (1 / (normalized_like_perc - min_normalized_perc + 1 + 1e-9) + 1 / (normalized_watch_perc - min_normalized_perc + 1 + 1e-9)) AS within_region_popularity_score
    FROM
        offset_stats
    ORDER BY
        region, within_region_popularity_score DESC
)

select * from region_popular_videos
"""

def create_region_popular_videos_l7d():
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()

with DAG('region_popular_videos_l7d', default_args=default_args, schedule_interval='10 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=create_region_popular_videos_l7d,
        on_failure_callback=send_alert_to_google_chat
    )