from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import requests

from user_base_facts.clickhouse_utils import (
    add_updated_at,
    clickhouse_command,
    clickhouse_insert,
    clickhouse_table_row_count,
)


def send_alert_to_google_chat():
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": f"DAG global_popular_videos_l90d failed."
    }
    requests.post(webhook_url, json=message)

def ensure_clickhouse_table():
    clickhouse_command(
        """
        CREATE TABLE IF NOT EXISTS yral.user_base_facts ON CLUSTER yral_cluster
        (
            `user_id` String,
            `region` Nullable(String),
            `occurrence_date` Nullable(Date),
            `last_updated_date` Date,
            `_updated_at` DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/yral/user_base_facts', '{replica}', _updated_at)
        ORDER BY user_id
        SETTINGS index_granularity = 8192
        """
    )


def updaet_or_init_ubf_table():
    ensure_clickhouse_table()
    is_bootstrap = clickhouse_table_row_count("user_base_facts") == 0
    source_filter = ""
    if not is_bootstrap:
        source_filter = "WHERE PARSE_DATE('%Y%m%d', CAST(last_updated_date AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)"

    client = bigquery.Client()
    rows_iter = client.query(
        """
        SELECT
            user_id,
            geo.region AS region,
            occurrence_date,
            PARSE_DATE('%Y%m%d', CAST(last_updated_date AS STRING)) AS last_updated_date
        FROM (
            SELECT
                user_id,
                geo,
                occurrence_date,
                last_updated_date,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_updated_date DESC) AS row_num
            FROM `hot-or-not-feed-intelligence.analytics_434929785.users_*`
            {source_filter}
        )
        WHERE row_num = 1
        """.format(source_filter=source_filter)
    ).result()
    data = add_updated_at([dict(row) for row in rows_iter])
    if not data:
        return
    clickhouse_insert(table="user_base_facts", data=data)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('user_base_facts', default_args=default_args, schedule_interval='0 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=updaet_or_init_ubf_table,
        on_failure_callback=send_alert_to_google_chat
    )
