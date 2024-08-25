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
        "text": f"DAG user_video_metrics_dag failed."
    }
    requests.post(webhook_url, json=message)

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'user_video_metrics'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def create_initial_query():
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.user_video_metrics` AS
WITH base_stats AS (
  SELECT
    user_id,
    AVG(CASE WHEN liked THEN 1 ELSE 0 END) AS user_like_avg,
    STDDEV(CASE WHEN liked THEN 1 ELSE 0 END) AS user_like_stddev,
    SUM(CASE WHEN liked THEN 1 ELSE 0 END) AS total_likes,
    AVG(CASE WHEN shared THEN 1 ELSE 0 END) AS user_share_avg,
    STDDEV(CASE WHEN shared THEN 1 ELSE 0 END) AS user_share_stddev,
    SUM(CASE WHEN shared THEN 1 ELSE 0 END) AS total_shares,
    AVG(mean_percentage_watched) AS user_watch_percentage_avg,
    STDDEV(mean_percentage_watched) AS user_watch_percentage_stddev,
    COUNT(mean_percentage_watched) AS total_watches,
    MAX(last_watched_timestamp) AS last_update_timestamp
  FROM
    `hot-or-not-feed-intelligence.yral_ds.userVideoRelation`
  GROUP BY user_id
)
SELECT
    user_id,
    user_like_avg,
    user_like_stddev,
    user_like_avg * (1 - user_like_avg) AS user_normalized_like_avg,
    (1 - user_like_avg) * user_like_stddev AS user_normalized_like_stddev,
    total_likes,
    user_share_avg,
    user_share_stddev,
    user_share_avg * (1 - user_share_avg) AS user_normalized_share_avg,
    (1 - user_share_avg) * user_share_stddev AS user_normalized_share_stddev,
    total_shares,
    user_watch_percentage_avg,
    user_watch_percentage_stddev,
    user_watch_percentage_avg * (1 - user_watch_percentage_avg) AS user_normalized_watch_percentage_avg,
    (1 - user_watch_percentage_avg) * user_watch_percentage_stddev AS user_normalized_watch_percentage_stddev,
    total_watches,
    last_update_timestamp
FROM
    base_stats;

    """

def create_incremental_query():
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.user_video_metrics` T
USING (
  SELECT
    user_id,
    AVG(CASE WHEN liked THEN 1 ELSE 0 END) AS user_like_avg,
    STDDEV(CASE WHEN liked THEN 1 ELSE 0 END) AS user_like_stddev,
    SUM(CASE WHEN liked THEN 1 ELSE 0 END) AS total_likes,
    AVG(CASE WHEN shared THEN 1 ELSE 0 END) AS user_share_avg,
    STDDEV(CASE WHEN shared THEN 1 ELSE 0 END) AS user_share_stddev,
    SUM(CASE WHEN shared THEN 1 ELSE 0 END) AS total_shares,
    AVG(mean_percentage_watched) AS user_watch_percentage_avg,
    STDDEV(mean_percentage_watched) AS user_watch_percentage_stddev,
    COUNT(mean_percentage_watched) AS total_watches,
    MAX(last_watched_timestamp) AS last_update_timestamp
  FROM
    `hot-or-not-feed-intelligence.yral_ds.userVideoRelation`
  WHERE
    last_watched_timestamp > (
      SELECT MAX(last_update_timestamp) 
      FROM `hot-or-not-feed-intelligence.yral_ds.user_video_metrics`
    )
  GROUP BY user_id
) S
ON T.user_id = S.user_id
WHEN MATCHED THEN
  UPDATE SET 
    T.user_like_avg = (
      T.user_like_avg * T.total_watches + S.user_like_avg * S.total_watches
    ) / (T.total_watches + S.total_watches),
    T.user_like_stddev = SQRT(
      (
        (T.total_watches - 1) * POW(T.user_like_stddev, 2) + 
        (S.total_watches - 1) * POW(S.user_like_stddev, 2) + 
        (T.total_watches * S.total_watches / (T.total_watches + S.total_watches)) * 
        POW(T.user_like_avg - S.user_like_avg, 2)
      ) / (T.total_watches + S.total_watches - 1)
    ),
    T.total_likes = T.total_likes + S.total_likes,
    T.user_share_avg = (
      T.user_share_avg * T.total_watches + S.user_share_avg * S.total_watches
    ) / (T.total_watches + S.total_watches),
    T.user_share_stddev = SQRT(
      (
        (T.total_watches - 1) * POW(T.user_share_stddev, 2) + 
        (S.total_watches - 1) * POW(S.user_share_stddev, 2) + 
        (T.total_watches * S.total_watches / (T.total_watches + S.total_watches)) * 
        POW(T.user_share_avg - S.user_share_avg, 2)
      ) / (T.total_watches + S.total_watches - 1)
    ),
    T.total_shares = T.total_shares + S.total_shares,
    T.user_watch_percentage_avg = (
      T.user_watch_percentage_avg * T.total_watches + S.user_watch_percentage_avg * S.total_watches
    ) / (T.total_watches + S.total_watches),
    T.user_watch_percentage_stddev = SQRT(
      (
        (T.total_watches - 1) * POW(T.user_watch_percentage_stddev, 2) + 
        (S.total_watches - 1) * POW(S.user_watch_percentage_stddev, 2) + 
        (T.total_watches * S.total_watches / (T.total_watches + S.total_watches)) * 
        POW(T.user_watch_percentage_avg - S.user_watch_percentage_avg, 2)
      ) / (T.total_watches + S.total_watches - 1)
    ),
    T.total_watches = T.total_watches + S.total_watches,
    T.user_normalized_like_avg = T.user_like_avg * (1 - T.user_like_avg),
    T.user_normalized_like_stddev = (1 - T.user_like_avg) * T.user_like_stddev,
    T.user_normalized_share_avg = T.user_share_avg * (1 - T.user_share_avg),
    T.user_normalized_share_stddev = (1 - T.user_share_avg) * T.user_share_stddev,
    T.user_normalized_watch_percentage_avg = T.user_watch_percentage_avg * (1 - T.user_watch_percentage_avg),
    T.user_normalized_watch_percentage_stddev = (1 - T.user_watch_percentage_avg) * T.user_watch_percentage_stddev,
    T.last_update_timestamp = S.last_update_timestamp
WHEN NOT MATCHED THEN
  INSERT (
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
    last_update_timestamp
  )
  VALUES (
    S.user_id, 
    S.user_like_avg, 
    S.user_like_stddev, 
    S.total_likes, 
    S.user_share_avg, 
    S.user_share_stddev, 
    S.total_shares, 
    S.user_watch_percentage_avg, 
    S.user_watch_percentage_stddev, 
    S.total_watches, 
    S.user_like_avg * (1 - S.user_like_avg), 
    (1 - S.user_like_avg) * S.user_like_stddev, 
    S.user_share_avg * (1 - S.user_share_avg), 
    (1 - S.user_share_avg) * S.user_share_stddev, 
    S.user_watch_percentage_avg * (1 - S.user_watch_percentage_avg), 
    (1 - S.user_watch_percentage_avg) * S.user_watch_percentage_stddev, 
    S.last_update_timestamp
  )
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

with DAG('user_video_metrics', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )