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
        "text": f"DAG video_statistics_dag failed."
    }
    requests.post(webhook_url, json=message)

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'video_statistics'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def create_initial_query():
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.video_statistics` AS
WITH user_contributions AS (
  SELECT
    uvr.video_id,
    uvr.user_id,
    (1 - IFNULL(user_like_avg, 1)) * CAST(liked AS INT64) AS user_normalized_like_contribution,
    (1 - IFNULL(user_share_avg, 1)) * CAST(shared AS INT64) AS user_normalized_share_contribution,
    ((1 - IFNULL(user_watch_percentage_avg, 1))) * mean_percentage_watched AS user_normalized_watch_contribution,
    last_watched_timestamp
  FROM
    `hot-or-not-feed-intelligence.yral_ds.userVideoRelation` uvr
  LEFT JOIN
    `hot-or-not-feed-intelligence.yral_ds.user_video_metrics` uvm
  ON
    uvr.user_id = uvm.user_id -- the more recent videos the better 
),
base AS (
  SELECT
    video_id,
    AVG(user_normalized_like_contribution) AS user_normalized_like_perc,
    AVG(user_normalized_share_contribution) AS user_normalized_share_perc,
    AVG(user_normalized_watch_contribution) AS user_normalized_watch_percentage_perc,
    COUNT(user_id) AS total_impressions,
    MAX(last_watched_timestamp) AS last_update_timestamp
  FROM
    user_contributions
  GROUP BY
    video_id
)
select 
  video_id,
  user_normalized_like_perc,
  user_normalized_share_perc,
  user_normalized_watch_percentage_perc,
  total_impressions,
  last_update_timestamp
from base
    """

def create_incremental_query():
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.video_statistics` T
USING (
  WITH user_contributions AS (
    select
    uvr.video_id,
    uvr.user_id,
    (1 - IFNULL(user_like_avg, 1)) * CAST(liked AS INT64) AS user_normalized_like_contribution,
    (1 - IFNULL(user_share_avg, 1)) * CAST(shared AS INT64) AS user_normalized_share_contribution,
    ((1 - IFNULL(user_watch_percentage_avg, 1))) * mean_percentage_watched AS user_normalized_watch_contribution,
    last_watched_timestamp
  FROM
    `hot-or-not-feed-intelligence.yral_ds.userVideoRelation` uvr
  LEFT JOIN
    `hot-or-not-feed-intelligence.yral_ds.user_video_metrics` uvm
      ON
        uvr.user_id = uvm.user_id -- the more recent videos the better 
    WHERE
      last_watched_timestamp > (SELECT MAX(last_update_timestamp) FROM `hot-or-not-feed-intelligence.yral_ds.video_statistics`)
  )
  SELECT
    video_id,
    AVG(user_normalized_like_contribution) AS user_normalized_like_perc,
    AVG(user_normalized_share_contribution) AS user_normalized_share_perc,
    AVG(user_normalized_watch_contribution) AS user_normalized_watch_percentage_perc,
    COUNT(user_id) AS total_impressions,
    MAX(last_watched_timestamp) AS last_update_timestamp
  FROM
    user_contributions
  GROUP BY
    video_id
) S
ON T.video_id = S.video_id
WHEN MATCHED THEN
  UPDATE SET 
    T.user_normalized_like_perc = (T.user_normalized_like_perc * T.total_impressions + S.user_normalized_like_perc * S.total_impressions) / (T.total_impressions + S.total_impressions),
    T.user_normalized_share_perc = (T.user_normalized_share_perc * T.total_impressions + S.user_normalized_share_perc * S.total_impressions) / (T.total_impressions + S.total_impressions),
    T.user_normalized_watch_percentage_perc = (T.user_normalized_watch_percentage_perc * T.total_impressions + S.user_normalized_watch_percentage_perc * S.total_impressions) / (T.total_impressions + S.total_impressions),
    T.total_impressions = T.total_impressions + S.total_impressions,
    T.last_update_timestamp = S.last_update_timestamp
WHEN NOT MATCHED THEN
  INSERT (video_id, user_normalized_like_perc, user_normalized_share_perc, user_normalized_watch_percentage_perc, total_impressions, last_update_timestamp)
  VALUES (S.video_id, S.user_normalized_like_perc, S.user_normalized_share_perc, S.user_normalized_watch_percentage_perc, S.total_impressions, S.last_update_timestamp); 
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

with DAG('video_statistics', default_args=default_args, schedule_interval='*/35 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )