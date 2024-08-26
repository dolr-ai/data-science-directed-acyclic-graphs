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
        "text": f"DAG video_statistics_normalized_dag failed."
    }
    requests.post(webhook_url, json=message)

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'video_statistics_normalized'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def create_initial_query():
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.video_statistics_normalized` AS
WITH global_stats AS (
  SELECT
    global_avg_user_normalized_likes,
    global_stddev_user_normalized_likes,
    global_avg_user_normalized_shares,
    global_stddev_user_normalized_shares,
    global_avg_user_normalized_watch_percentage,
    global_stddev_user_normalized_watch_percentage
  FROM
    `hot-or-not-feed-intelligence.yral_ds.global_video_stats`
),
normalized_stats AS (
  SELECT
    vs.video_id,
    user_normalized_like_perc as like_percentage_un,
    user_normalized_share_perc as share_percentage_un,
    user_normalized_watch_percentage_perc as watch_percentage_un,
    (vs.user_normalized_like_perc - gs.global_avg_user_normalized_likes) / gs.global_stddev_user_normalized_likes AS normalized_like_perc,
    (vs.user_normalized_share_perc - gs.global_avg_user_normalized_shares) / gs.global_stddev_user_normalized_shares AS normalized_share_perc,
    (vs.user_normalized_watch_percentage_perc - gs.global_avg_user_normalized_watch_percentage) / gs.global_stddev_user_normalized_watch_percentage AS normalized_watch_perc,
    vs.total_impressions,
    vs.last_update_timestamp
  FROM
    `hot-or-not-feed-intelligence.yral_ds.video_statistics` vs,
    global_stats gs
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
  last_update_timestamp
FROM
  normalized_stats;
    """

def create_incremental_query():
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.video_statistics_normalized` T
USING (
  WITH global_stats AS (
    SELECT
      global_avg_user_normalized_likes,
      global_stddev_user_normalized_likes,
      global_avg_user_normalized_shares,
      global_stddev_user_normalized_shares,
      global_avg_user_normalized_watch_percentage,
      global_stddev_user_normalized_watch_percentage
    FROM
      `hot-or-not-feed-intelligence.yral_ds.global_video_stats`
  ),
  normalized_stats AS (
    SELECT
      vs.video_id,
      (vs.user_normalized_like_perc - gs.global_avg_user_normalized_likes) / gs.global_stddev_user_normalized_likes AS normalized_like_perc,
      (vs.user_normalized_share_perc - gs.global_avg_user_normalized_shares) / gs.global_stddev_user_normalized_shares AS normalized_share_perc,
      (vs.user_normalized_watch_percentage_perc - gs.global_avg_user_normalized_watch_percentage) / gs.global_stddev_user_normalized_watch_percentage AS normalized_watch_perc,
      vs.total_impressions,
      vs.last_update_timestamp
    FROM
      `hot-or-not-feed-intelligence.yral_ds.video_statistics` vs,
      global_stats gs
    WHERE
      vs.last_update_timestamp > (SELECT MAX(last_update_timestamp) FROM `hot-or-not-feed-intelligence.yral_ds.video_statistics_normalized`)
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
    last_update_timestamp
  FROM
    normalized_stats
) S
ON T.video_id = S.video_id
WHEN MATCHED THEN
  UPDATE SET 
    T.like_percentage_un = (T.like_percentage_un * T.total_impressions + S.like_percentage_un * S.total_impressions) / (T.total_impressions + S.total_impressions),
    T.share_percentage_un = (T.share_percentage_un * T.total_impressions + S.share_percentage_un * S.total_impressions) / (T.total_impressions + S.total_impressions),
    T.watch_percentage_un = (T.watch_percentage_un * T.total_impressions + S.watch_percentage_un * S.total_impressions) / (T.total_impressions + S.total_impressions),
    T.normalized_like_perc = (T.like_percentage_un - gs.global_avg_user_normalized_likes) / gs.global_stddev_user_normalized_likes,
    T.normalized_share_perc = (T.share_percentage_un - gs.global_avg_user_normalized_shares) / gs.global_stddev_user_normalized_shares,
    T.normalized_watch_perc = (T.watch_percentage_un - gs.global_avg_user_normalized_watch_percentage) / gs.global_stddev_user_normalized_watch_percentage,
    T.total_impressions = T.total_impressions + S.total_impressions,
    T.last_update_timestamp = S.last_update_timestamp
WHEN NOT MATCHED THEN
  INSERT (video_id, like_percentage_un, share_percentage_un, watch_percentage_un, normalized_like_perc, normalized_share_perc, normalized_watch_perc, total_impressions, last_update_timestamp)
  VALUES (S.video_id, S.like_percentage_un, S.share_percentage_un, S.watch_percentage_un, S.normalized_like_perc, S.normalized_share_perc, S.normalized_watch_perc, S.total_impressions, S.last_update_timestamp);
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

with DAG('video_statistics_normalized', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )