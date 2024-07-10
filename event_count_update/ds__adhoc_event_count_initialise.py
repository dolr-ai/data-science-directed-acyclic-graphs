from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'ds__adhoc_event_count_init',
    default_args=default_args,
    description='A temporary DAG to run BigQuery aggregation',
    schedule_interval=None,
)

# Your SQL query as a string
sql_query = """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.analytics_views.daily_event_counts_table` AS
WITH dated_events AS (
  SELECT 
    DATE(timestamp) AS event_date,
    event as event_name,
    *
  FROM 
    `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
),

last_10_days AS (
  SELECT DISTINCT event_dateMERGE `hot-or-not-feed-intelligence.analytics_views.daily_event_counts_table` T
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
  FROM dated_events
  ORDER BY event_date DESC
  LIMIT 100
)

SELECT 
  d.event_date,
  d.event_name,
  COUNT(*) AS event_count
FROM 
  dated_events d
JOIN 
  last_10_days l ON d.event_date = l.event_date
GROUP BY 
  d.event_date,
  d.event_name
ORDER BY 
  d.event_date DESC,
  d.event_name
"""

run_query = BigQueryExecuteQueryOperator(
    task_id='run_query',
    sql=sql_query,
    use_legacy_sql=False,
    dag=dag,
)

run_query 

