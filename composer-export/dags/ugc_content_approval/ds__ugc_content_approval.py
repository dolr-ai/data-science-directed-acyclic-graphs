"""
Temporary bridge DAG to mirror ugc_content_approval from BigQuery to ClickHouse.

This is a full-refresh sync that should be kept only until the real writer
for ugc_content_approval is identified and moved to the ClickHouse path directly.
"""

import logging
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery

from ugc_content_approval.clickhouse_utils import add_updated_at, clickhouse_insert, get_clickhouse_client

logger = logging.getLogger(__name__)

PROJECT_ID = "hot-or-not-feed-intelligence"
DATASET_ID = "yral_ds"
TABLE_NAME = "ugc_content_approval"
DAG_ID = "sync_ugc_content_approval_to_clickhouse"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def send_alert_to_google_chat(context=None, text=None):
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG sync_ugc_content_approval_to_clickhouse failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for %s", DAG_ID)


def sync_to_clickhouse():
    """Full refresh the ClickHouse ugc_content_approval bridge table from BigQuery."""
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        rows_iter = bq_client.query(
            """
            SELECT
                video_id,
                post_id,
                canister_id,
                user_id,
                is_approved,
                created_at
            FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
            ORDER BY video_id
            """
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])

        ch_client = get_clickhouse_client()
        ch_client.command("TRUNCATE TABLE yral.ugc_content_approval")

        if not data:
            logger.warning("ugc_content_approval: truncated ClickHouse table; no rows to insert")
            return

        inserted = clickhouse_insert(
            table=TABLE_NAME,
            data=data,
            client=ch_client,
        )
        logger.info("ugc_content_approval: rebuilt %s rows in ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse bridge sync failed for ugc_content_approval")
        send_alert_to_google_chat(
            text=(
                "ClickHouse bridge sync failed for ugc_content_approval. "
                "The DAG should be investigated before the bridge is trusted."
            )
        )
        raise


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    sync_task = PythonOperator(
        task_id="sync_to_clickhouse",
        python_callable=sync_to_clickhouse,
    )
