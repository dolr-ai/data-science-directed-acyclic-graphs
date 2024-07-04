from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_event_count_update',
    default_args=default_args,
    description='A DAG to update daily event counts in BigQuery',
    schedule_interval='0 0 * * *',
    catchup=False,
)

sql_query = """
MERGE `hot-or-not-feed-intelligence.analytics_views.daily_event_counts_table` T
USING (
  WITH dated_events AS (
    SELECT 
      DATE(timestamp) AS event_date,
      event as event_name,
      *
    FROM 
      `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
    WHERE 
      DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  )
  SELECT 
    event_date,
    event_name,
    COUNT(*) AS event_count
  FROM 
    dated_events
  GROUP BY 
    event_date,
    event_name
) S
ON T.event_date = S.event_date AND T.event_name = S.event_name
WHEN MATCHED THEN
  UPDATE SET event_count = S.event_count
WHEN NOT MATCHED THEN
  INSERT (event_date, event_name, event_count)
  VALUES (S.event_date, S.event_name, S.event_count)
"""

run_query = BigQueryExecuteQueryOperator(
    task_id='run_query',
    sql=sql_query,
    use_legacy_sql=False,
    dag=dag,
)

run_query