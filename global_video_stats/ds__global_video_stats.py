from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from google.cloud import bigquery
import requests

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG global_video_stats_dag failed."
    }
    requests.post(webhook_url, json=message)

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'global_video_stats'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def create_initial_query():
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.global_video_stats` AS
SELECT
  AVG(user_normalized_like_perc) AS global_avg_user_normalized_likes,
  STDDEV(user_normalized_like_perc) AS global_stddev_user_normalized_likes,
  AVG(user_normalized_share_perc) AS global_avg_user_normalized_shares,
  STDDEV(user_normalized_share_perc) AS global_stddev_user_normalized_shares,
  AVG(user_normalized_watch_percentage_perc) AS global_avg_user_normalized_watch_percentage,
  STDDEV(user_normalized_watch_percentage_perc) AS global_stddev_user_normalized_watch_percentage,
  SUM(total_impressions) AS total_impressions,
  MAX(last_update_timestamp) AS last_update_timestamp
FROM
  `hot-or-not-feed-intelligence.yral_ds.video_statistics`;
    """

def create_incremental_query():
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.global_video_stats` T
USING (
  SELECT
    AVG(user_normalized_like_perc) AS global_avg_user_normalized_likes,
    STDDEV(user_normalized_like_perc) AS global_stddev_user_normalized_likes,
    AVG(user_normalized_share_perc) AS global_avg_user_normalized_shares,
    STDDEV(user_normalized_share_perc) AS global_stddev_user_normalized_shares,
    AVG(user_normalized_watch_percentage_perc) AS global_avg_user_normalized_watch_percentage,
    STDDEV(user_normalized_watch_percentage_perc) AS global_stddev_user_normalized_watch_percentage,
    SUM(total_impressions) AS total_impressions,
    MAX(last_update_timestamp) AS last_update_timestamp
  FROM
    `hot-or-not-feed-intelligence.yral_ds.video_statistics`
  WHERE
    last_update_timestamp > (SELECT MAX(last_update_timestamp) FROM `hot-or-not-feed-intelligence.yral_ds.global_video_stats`)
    AND total_impressions IS NOT NULL
) S
ON TRUE -- Always match to update the global stats
WHEN MATCHED THEN
  UPDATE SET 
    T.global_avg_user_normalized_likes = IFNULL((T.global_avg_user_normalized_likes * T.total_impressions + S.global_avg_user_normalized_likes * S.total_impressions) / (T.total_impressions + S.total_impressions), T.global_avg_user_normalized_likes),
    T.global_stddev_user_normalized_likes = IFNULL(SQRT(
      (
        (T.total_impressions - 1) * POW(T.global_stddev_user_normalized_likes, 2) + 
        (S.total_impressions - 1) * POW(S.global_stddev_user_normalized_likes, 2) + 
        (T.total_impressions * S.total_impressions / (T.total_impressions + S.total_impressions)) * POW(T.global_avg_user_normalized_likes - S.global_avg_user_normalized_likes, 2)
      ) / (T.total_impressions + S.total_impressions - 1)
    ), T.global_stddev_user_normalized_likes),
    T.global_avg_user_normalized_shares = IFNULL((T.global_avg_user_normalized_shares * T.total_impressions + S.global_avg_user_normalized_shares * S.total_impressions) / (T.total_impressions + S.total_impressions), T.global_avg_user_normalized_shares),
    T.global_stddev_user_normalized_shares = IFNULL(SQRT(
      (
        (T.total_impressions - 1) * POW(T.global_stddev_user_normalized_shares, 2) + 
        (S.total_impressions - 1) * POW(S.global_stddev_user_normalized_shares, 2) + 
        (T.total_impressions * S.total_impressions / (T.total_impressions + S.total_impressions)) * POW(T.global_avg_user_normalized_shares - S.global_avg_user_normalized_shares, 2)
      ) / (T.total_impressions + S.total_impressions - 1)
    ), T.global_stddev_user_normalized_shares),
    T.global_avg_user_normalized_watch_percentage = IFNULL((T.global_avg_user_normalized_watch_percentage * T.total_impressions + S.global_avg_user_normalized_watch_percentage * S.total_impressions) / (T.total_impressions + S.total_impressions), T.global_avg_user_normalized_watch_percentage),
    T.global_stddev_user_normalized_watch_percentage = IFNULL(SQRT(
      (
        (T.total_impressions - 1) * POW(T.global_stddev_user_normalized_watch_percentage, 2) + 
        (S.total_impressions - 1) * POW(S.global_stddev_user_normalized_watch_percentage, 2) + 
        (T.total_impressions * S.total_impressions / (T.total_impressions + S.total_impressions)) * POW(T.global_avg_user_normalized_watch_percentage - S.global_avg_user_normalized_watch_percentage, 2)
      ) / (T.total_impressions + S.total_impressions - 1)
    ), T.global_stddev_user_normalized_watch_percentage),
    T.total_impressions = IFNULL(T.total_impressions, 0) + IFNULL(S.total_impressions, 0),
    T.last_update_timestamp = IFNULL(S.last_update_timestamp, T.last_update_timestamp)
WHEN NOT MATCHED THEN
  INSERT (global_avg_user_normalized_likes, global_stddev_user_normalized_likes, global_avg_user_normalized_shares, global_stddev_user_normalized_shares, global_avg_user_normalized_watch_percentage, global_stddev_user_normalized_watch_percentage, total_impressions, last_update_timestamp)
  VALUES (S.global_avg_user_normalized_likes, S.global_stddev_user_normalized_likes, S.global_avg_user_normalized_shares, S.global_stddev_user_normalized_shares, S.global_avg_user_normalized_watch_percentage, S.global_stddev_user_normalized_watch_percentage, S.total_impressions, S.last_update_timestamp);
"""

def run_query():
    if check_table_exists():
        query = create_incremental_query()
    else:
        query = create_initial_query()
    
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()

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