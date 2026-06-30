from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import bigquery
import logging
import requests

from follower_graph.clickhouse_utils import (
    add_updated_at,
    clickhouse_insert,
    clickhouse_max_timestamp_ms,
    clickhouse_table_row_count,
)

logger = logging.getLogger(__name__)


def send_alert_to_google_chat(context=None, text=None):
    """Sends failure alert to Google Chat webhook.

    Algorithm:
    1. Defines the webhook URL for Google Chat notifications
    2. Creates a message payload with DAG failure information
    3. Posts the message to the webhook
    """
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": text or "DAG ds__follower_graph failed."
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception:
        logger.exception("Failed to send Google Chat alert for ds__follower_graph")


def run_query():
    try:
        if clickhouse_table_row_count("follower_graph") == 0:
            raise RuntimeError(
                "ClickHouse bootstrap missing for yral.follower_graph; "
                "complete bulk load before enabling ClickHouse-only writes"
            )

        client = bigquery.Client()
        lower_bound_ms = clickhouse_max_timestamp_ms("follower_graph", "last_updated_timestamp")
        if lower_bound_ms is None:
            raise RuntimeError("follower_graph ClickHouse watermark is NULL; bootstrap state is invalid")

        overlap_ms = 60 * 60 * 1000
        lower_bound_ms = max(lower_bound_ms - overlap_ms, 0)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lower_bound_ms", "INT64", lower_bound_ms),
            ]
        )
        rows_iter = client.query(
            """
            SELECT
              JSON_EXTRACT_SCALAR(params, '$.user_id') AS follower_id,
              JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS following_id,
              CASE WHEN event = 'mp_user_followed' THEN TRUE ELSE FALSE END AS active,
              timestamp AS last_updated_timestamp
            FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
            WHERE event IN ('mp_user_followed', 'mp_user_unfollowed')
              AND timestamp >= TIMESTAMP_MILLIS(@lower_bound_ms)
              AND JSON_EXTRACT_SCALAR(params, '$.user_id') IS NOT NULL
              AND JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY
                JSON_EXTRACT_SCALAR(params, '$.user_id'),
                JSON_EXTRACT_SCALAR(params, '$.publisher_user_id')
              ORDER BY timestamp DESC
            ) = 1
            ORDER BY last_updated_timestamp, follower_id, following_id
            """,
            job_config=job_config,
        ).result()
        data = add_updated_at([dict(row) for row in rows_iter])
        if not data:
            logger.info("follower_graph: no new source rows to write")
            return

        inserted = clickhouse_insert(table="follower_graph", data=data)
        logger.info("follower_graph: wrote %s rows directly to ClickHouse", inserted)
    except Exception:
        logger.exception("ClickHouse write failed for follower_graph")
        send_alert_to_google_chat(text="ClickHouse write failed for ds__follower_graph.")
        raise


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ds__follower_graph',
    default_args=default_args,
    description='A DAG to track follower-following relationships from follow/unfollow events',
    schedule_interval='*/5 * * * *',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    run_query_task = PythonOperator(
        task_id='update_follower_graph',
        python_callable=run_query,
        on_failure_callback=send_alert_to_google_chat
    )



if __name__ == "__main__":
    run_query()
