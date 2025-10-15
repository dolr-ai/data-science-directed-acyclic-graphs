from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import requests

def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG video_upload_stats_update failed."
    }
    requests.post(webhook_url, json=message)

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'video_upload_stats'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def create_initial_query():
    return """
CREATE OR REPLACE TABLE `yral_ds.video_upload_stats` 
PARTITION BY DATE(timestamp)
CLUSTER BY timestamp, video_id
AS (
    WITH video_upload_success_events AS (
        SELECT 
            JSON_EXTRACT_SCALAR(data, "$.event_data['$device_id']") AS device_id,
            JSON_EXTRACT_SCALAR(data, "$.event_data['$os']") AS os,
            JSON_EXTRACT_SCALAR(data, "$.event_data['$user_id']") AS user_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.btc_balance_e8s') AS btc_balance_e8s,
            JSON_EXTRACT_SCALAR(data, '$.event_data.city') AS city,
            JSON_EXTRACT_SCALAR(data, '$.event_data.country') AS country,
            JSON_EXTRACT_SCALAR(data, '$.event_data.creator_commision_percentage') AS creator_commision_percentage,
            JSON_EXTRACT_SCALAR(data, '$.event_data.custom_device_id') AS custom_device_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.device') AS device,
            JSON_EXTRACT_SCALAR(data, '$.event_data.distinct_id') AS distinct_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.event') AS event,
            JSON_EXTRACT_SCALAR(data, '$.event_data.game_type') AS game_type,
            JSON_EXTRACT_SCALAR(data, '$.event_data.ip') AS ip,
            JSON_EXTRACT_SCALAR(data, '$.event_data.ip_addr') AS ip_addr,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_creator') AS is_creator,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_game_enabled') AS is_game_enabled,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_logged_in') AS is_logged_in,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_nsfw_enabled') AS is_nsfw_enabled,
            JSON_EXTRACT_SCALAR(data, '$.event_data.principal') AS principal,
            JSON_EXTRACT_SCALAR(data, '$.event_data.region') AS region,
            JSON_EXTRACT_SCALAR(data, '$.event_data.sats_balance') AS sats_balance,
            JSON_EXTRACT_SCALAR(data, '$.event_data.user_agent') AS user_agent,
            JSON_EXTRACT_SCALAR(data, '$.event_data.video_id') AS video_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.visitor_id') AS visitor_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.upload_type') AS upload_type,
            PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(data, '$.timestamp')) AS timestamp
        FROM `yral_ds.analytics_events`
        WHERE JSON_EXTRACT_SCALAR(data, '$.event_data.event') = 'video_upload_success'
    )
    SELECT * FROM video_upload_success_events
)
"""

def create_incremental_query():
    return """
MERGE `yral_ds.video_upload_stats` AS target
USING (
    WITH last_ts AS (
        SELECT MAX(timestamp) AS max_ts FROM `yral_ds.video_upload_stats`
    ),
    video_upload_success_events AS (
        SELECT 
            JSON_EXTRACT_SCALAR(data, "$.event_data['$device_id']") AS device_id,
            JSON_EXTRACT_SCALAR(data, "$.event_data['$os']") AS os,
            JSON_EXTRACT_SCALAR(data, "$.event_data['$user_id']") AS user_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.btc_balance_e8s') AS btc_balance_e8s,
            JSON_EXTRACT_SCALAR(data, '$.event_data.city') AS city,
            JSON_EXTRACT_SCALAR(data, '$.event_data.country') AS country,
            JSON_EXTRACT_SCALAR(data, '$.event_data.creator_commision_percentage') AS creator_commision_percentage,
            JSON_EXTRACT_SCALAR(data, '$.event_data.custom_device_id') AS custom_device_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.device') AS device,
            JSON_EXTRACT_SCALAR(data, '$.event_data.distinct_id') AS distinct_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.event') AS event,
            JSON_EXTRACT_SCALAR(data, '$.event_data.game_type') AS game_type,
            JSON_EXTRACT_SCALAR(data, '$.event_data.ip') AS ip,
            JSON_EXTRACT_SCALAR(data, '$.event_data.ip_addr') AS ip_addr,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_creator') AS is_creator,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_game_enabled') AS is_game_enabled,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_logged_in') AS is_logged_in,
            JSON_EXTRACT_SCALAR(data, '$.event_data.is_nsfw_enabled') AS is_nsfw_enabled,
            JSON_EXTRACT_SCALAR(data, '$.event_data.principal') AS principal,
            JSON_EXTRACT_SCALAR(data, '$.event_data.region') AS region,
            JSON_EXTRACT_SCALAR(data, '$.event_data.sats_balance') AS sats_balance,
            JSON_EXTRACT_SCALAR(data, '$.event_data.user_agent') AS user_agent,
            JSON_EXTRACT_SCALAR(data, '$.event_data.video_id') AS video_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.visitor_id') AS visitor_id,
            JSON_EXTRACT_SCALAR(data, '$.event_data.upload_type') AS upload_type,
            PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(data, '$.timestamp')) AS timestamp
        FROM `yral_ds.analytics_events`, last_ts
        WHERE JSON_EXTRACT_SCALAR(data, '$.event_data.event') = 'video_upload_success'
          AND PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(data, '$.timestamp')) > IFNULL(last_ts.max_ts, TIMESTAMP('1970-01-01'))
    )
    SELECT * FROM video_upload_success_events
) AS source
ON target.timestamp = source.timestamp AND target.video_id = source.video_id
WHEN NOT MATCHED THEN
    INSERT ROW
"""

def update_video_upload_stats():
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
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ds__video_upload_stats_update',
    default_args=default_args,
    description='A DAG to update video upload stats in BigQuery',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    is_paused_upon_creation=True
) as dag:
    
    run_query_task = PythonOperator(
        task_id='update_video_upload_stats',
        python_callable=update_video_upload_stats,
        on_failure_callback=send_alert_to_google_chat
    )