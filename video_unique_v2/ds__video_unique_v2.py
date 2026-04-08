"""
Temporary bridge DAG to mirror video_unique_v2 from BigQuery to ClickHouse.

This is a full-refresh sync that should be kept only until the real writer
for video_unique_v2 is identified and moved to the ClickHouse path directly.
"""

import logging
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery

from video_unique_v2.clickhouse_utils import add_updated_at, clickhouse_insert, get_clickhouse_client

logger = logging.getLogger(__name__)

PROJECT_ID = "hot-or-not-feed-intelligence"
DATASET_ID = "yral_ds"
TABLE_NAME = "video_unique_v2"
DAG_ID = "sync_video_unique_v2_to_clickhouse"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def send_alert_to_google_chat(context=None, text=None):
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG sync_video_unique_v2_to_clickhouse failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for %s", DAG_ID)


def sync_to_clickhouse():
    """Full refresh the ClickHouse video_unique_v2 bridge table from BigQuery."""
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        rows_iter = bq_client.query(
            """
            SELECT
                video_id,
                videohash,
                created_at
            FROM `hot-or-not-feed-intelligence.yral_ds.video_unique_v2`
            ORDER BY video_id
            """
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])

        ch_client = get_clickhouse_client()
        ch_client.command("TRUNCATE TABLE yral.video_unique_v2")

        if not data:
            logger.warning("video_unique_v2: truncated ClickHouse table; no rows to insert")
            return

        inserted = clickhouse_insert(
            table=TABLE_NAME,
            data=data,
            client=ch_client,
        )
        logger.info("video_unique_v2: rebuilt %s rows in ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse bridge sync failed for video_unique_v2")
        send_alert_to_google_chat(
            text=(
                "ClickHouse bridge sync failed for video_unique_v2. "
                "The DAG should be investigated before the bridge is trusted."
            )
        )
        raise


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    sync_task = PythonOperator(
        task_id="sync_to_clickhouse",
        python_callable=sync_to_clickhouse,
    )

