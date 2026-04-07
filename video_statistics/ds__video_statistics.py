from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from google.cloud import bigquery
import logging
import requests

from video_statistics.clickhouse_utils import (
    add_updated_at,
    clickhouse_insert,
    clickhouse_max_timestamp_ms,
    clickhouse_table_row_count,
)

logger = logging.getLogger(__name__)

def send_alert_to_google_chat(context=None, text=None):
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG video_statistics_dag failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for video_statistics")

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


def sync_to_clickhouse():
    """Sync recently written video statistics rows from BigQuery to ClickHouse."""
    try:
        if clickhouse_table_row_count("video_statistics") == 0:
            raise RuntimeError(
                "ClickHouse bootstrap missing for yral.video_statistics; "
                "complete Phase 2 bulk load before enabling Phase 3 dual-write"
            )

        client = bigquery.Client()
        lower_bound_ms = clickhouse_max_timestamp_ms("video_statistics", "last_update_timestamp")
        if lower_bound_ms is None:
            raise RuntimeError("video_statistics ClickHouse watermark is NULL; bootstrap state is invalid")

        overlap_ms = 6 * 60 * 60 * 1000
        lower_bound_ms = max(lower_bound_ms - overlap_ms, 0)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lower_bound_ms", "INT64", lower_bound_ms),
            ]
        )
        rows_iter = client.query(
            """
            SELECT
                video_id,
                user_normalized_like_perc,
                user_normalized_share_perc,
                user_normalized_watch_percentage_perc,
                total_impressions,
                last_update_timestamp
            FROM `hot-or-not-feed-intelligence.yral_ds.video_statistics`
            WHERE last_update_timestamp >= TIMESTAMP_MILLIS(@lower_bound_ms)
            ORDER BY last_update_timestamp, video_id
            """,
            job_config=job_config,
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])
        if not data:
            logger.info("video_statistics: no recent rows to sync")
            return

        inserted = clickhouse_insert(table="video_statistics", data=data)
        logger.info("video_statistics: synced %s rows to ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse sync failed for video_statistics")
        send_alert_to_google_chat(
            text=(
                "ClickHouse sync failed for video_statistics, but the BigQuery write "
                "already completed. The DAG run will continue and ClickHouse catch-up is required."
            )
        )
        return

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

    sync_ch_task = PythonOperator(
        task_id='sync_to_clickhouse',
        python_callable=sync_to_clickhouse,
    )

    run_query_task >> sync_ch_task
