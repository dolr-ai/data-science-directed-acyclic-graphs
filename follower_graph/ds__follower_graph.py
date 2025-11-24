from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from google.cloud import bigquery
import requests


def send_alert_to_google_chat():
    """Sends failure alert to Google Chat webhook.

    Algorithm:
    1. Defines the webhook URL for Google Chat notifications
    2. Creates a message payload with DAG failure information
    3. Posts the message to the webhook
    """
    webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAkUFdZaw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=VC5HDNQgqVLbhRVQYisn_IO2WUAvrDeRV9_FTizccic"
    message = {
        "text": "DAG ds__follower_graph failed."
    }
    requests.post(webhook_url, json=message)


def check_table_exists():
    """Checks if the follower_graph table exists in BigQuery.

    Algorithm:
    1. Creates a BigQuery client
    2. Queries INFORMATION_SCHEMA to check for table existence
    3. Returns True if table exists, False otherwise

    Returns:
        bool: True if table exists, False otherwise
    """
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'follower_graph'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0


def create_initial_query():
    """Creates the initial SQL query to build follower_graph table from scratch.

    Algorithm:
    1. Creates or replaces the follower_graph table with partitioning and clustering
    2. Extracts all follow/unfollow events from analytics_events
    3. Uses QUALIFY ROW_NUMBER() to get the latest event per (follower_id, following_id) pair
    4. Filters out events with NULL user_id or publisher_user_id

    Returns:
        str: SQL query string for initial table creation

    Output Schema:
        follower_id: STRING - user_id from event (person doing the following)
        following_id: STRING - publisher_user_id from event (person being followed)
        active: BOOL - true if currently following, false if unfollowed
        last_updated_timestamp: TIMESTAMP - for watermarking incremental updates

    Table Properties:
        PARTITION BY DATE(last_updated_timestamp) - optimizes MAX() watermark query
        CLUSTER BY follower_id, following_id - optimizes "who does X follow?" queries
    """
    return """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.follower_graph`
PARTITION BY DATE(last_updated_timestamp)
CLUSTER BY follower_id, following_id
AS
SELECT
  JSON_EXTRACT_SCALAR(params, '$.user_id') AS follower_id,
  JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS following_id,
  CASE WHEN event = 'mp_user_followed' THEN TRUE ELSE FALSE END AS active,
  timestamp AS last_updated_timestamp
FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
WHERE event IN ('mp_user_followed', 'mp_user_unfollowed')
  AND JSON_EXTRACT_SCALAR(params, '$.user_id') IS NOT NULL
  AND JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    JSON_EXTRACT_SCALAR(params, '$.user_id'),
    JSON_EXTRACT_SCALAR(params, '$.publisher_user_id')
  ORDER BY timestamp DESC
) = 1
"""


def create_incremental_query():
    """Creates the incremental SQL query to merge new follow/unfollow events.

    Algorithm:
    1. Uses CTE to fetch MAX(last_updated_timestamp) watermark from target table
    2. Queries new events since watermark from analytics_events
    3. Filters for mp_user_followed and mp_user_unfollowed events
    4. Uses QUALIFY ROW_NUMBER() to get latest event per pair in the batch
    5. Uses MERGE to upsert records:
       - WHEN MATCHED: Updates active and last_updated_timestamp
       - WHEN NOT MATCHED: Inserts new row

    Note: Single query with CTE - no separate Python query needed for watermark.
    Uses IFNULL to handle empty table case (defaults to 1970-01-01).

    Returns:
        str: SQL query string for incremental updates
    """
    return """
MERGE `hot-or-not-feed-intelligence.yral_ds.follower_graph` T
USING (
  WITH last_ts AS (
    SELECT MAX(last_updated_timestamp) as max_ts
    FROM `hot-or-not-feed-intelligence.yral_ds.follower_graph`
  )
  SELECT
    JSON_EXTRACT_SCALAR(params, '$.user_id') AS follower_id,
    JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS following_id,
    CASE WHEN event = 'mp_user_followed' THEN TRUE ELSE FALSE END AS active,
    timestamp AS last_updated_timestamp
  FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`, last_ts
  WHERE event IN ('mp_user_followed', 'mp_user_unfollowed')
    AND timestamp > IFNULL(last_ts.max_ts, TIMESTAMP('1970-01-01'))
    AND JSON_EXTRACT_SCALAR(params, '$.user_id') IS NOT NULL
    AND JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      JSON_EXTRACT_SCALAR(params, '$.user_id'),
      JSON_EXTRACT_SCALAR(params, '$.publisher_user_id')
    ORDER BY timestamp DESC
  ) = 1
) S
ON T.follower_id = S.follower_id AND T.following_id = S.following_id
WHEN MATCHED THEN
  UPDATE SET
    T.active = S.active,
    T.last_updated_timestamp = S.last_updated_timestamp
WHEN NOT MATCHED THEN
  INSERT (follower_id, following_id, active, last_updated_timestamp)
  VALUES (S.follower_id, S.following_id, S.active, S.last_updated_timestamp)
"""


def run_query():
    """Main function to update follower_graph table.

    Algorithm:
    1. Checks if the table exists using check_table_exists()
    2. If table exists: Runs incremental MERGE query (watermark fetched via CTE)
    3. If table doesn't exist: Runs initial CREATE TABLE query
    4. Executes the chosen query using BigQuery client
    5. Waits for query completion
    """
    if check_table_exists():
        query = create_incremental_query()
        print("Running incremental update query...")
    else:
        query = create_initial_query()
        print("Running initial table creation query...")

    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()
    print(f"Query completed successfully. Job ID: {query_job.job_id}")


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
