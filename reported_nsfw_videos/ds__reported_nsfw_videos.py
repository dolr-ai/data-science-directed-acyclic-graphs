"""
Retired BigQuery excluded_videos maintenance DAG.

Manual GChat-approved bans are now written directly by nsfw_detect:
    1. yral.excluded_videos gets exclusion_reason='banned'
    2. yral.video_nsfw_agg gets a legacy compatibility NSFW row

This DAG no longer reads analytics BigQuery events, no longer writes BigQuery
excluded_videos, and no longer syncs that table back to ClickHouse. It remains
as a manual validation DAG so deploys can confirm the ClickHouse tables required
by the direct-write path are present.
"""

import logging
from datetime import timedelta

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from reported_nsfw_videos.clickhouse_utils import clickhouse_scalar, clickhouse_table_row_count

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "excluded_videos": {
        "video_id",
        "excluded_at",
        "exclusion_reason",
        "_updated_at",
    },
    "video_nsfw_agg": {
        "video_id",
        "gcs_video_id",
        "nsfw_ec",
        "nsfw_gore",
        "is_nsfw",
        "probability",
        "created_at",
        "updated_at",
        "_updated_at",
    },
}


def send_alert_to_google_chat(context=None, text=None):
    """Send failure alert to Google Chat webhook."""
    _ = context
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {"text": text or "DAG ds__reported_nsfw_videos ClickHouse validation failed."}
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for ds__reported_nsfw_videos")


def _column_exists(table: str, column: str) -> bool:
    if table not in REQUIRED_COLUMNS or column not in REQUIRED_COLUMNS[table]:
        raise ValueError(f"unexpected ClickHouse column check: {table}.{column}")

    count = clickhouse_scalar(
        "SELECT count() FROM system.columns "
        f"WHERE database = 'yral' AND table = '{table}' AND name = '{column}'"
    )
    return int(count or 0) > 0


def validate_clickhouse_manual_ban_tables():
    """Validate the ClickHouse tables used by nsfw_detect manual-ban writes."""
    missing = []
    for table, columns in REQUIRED_COLUMNS.items():
        row_count = clickhouse_table_row_count(table)
        logger.info("yral.%s row count: %s", table, row_count)
        for column in sorted(columns):
            if not _column_exists(table, column):
                missing.append(f"yral.{table}.{column}")

    if missing:
        raise RuntimeError(f"missing required ClickHouse columns: {', '.join(missing)}")

    banned_count = clickhouse_scalar(
        "SELECT count() FROM yral.excluded_videos FINAL WHERE exclusion_reason = 'banned'"
    )
    logger.info("yral.excluded_videos banned row count: %s", banned_count)
    logger.info(
        "ds__reported_nsfw_videos is retired as a writer; manual bans are written by nsfw_detect"
    )


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ds__reported_nsfw_videos",
    default_args=default_args,
    description="Retired BigQuery excluded_videos DAG; validates ClickHouse manual-ban tables",
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True,
) as dag:
    validate_tables_task = PythonOperator(
        task_id="validate_clickhouse_manual_ban_tables",
        python_callable=validate_clickhouse_manual_ban_tables,
        on_failure_callback=send_alert_to_google_chat,
    )


if __name__ == "__main__":
    validate_clickhouse_manual_ban_tables()
