from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from google.cloud import bigquery

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.analytics_views.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'userVideoRelation'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def get_last_timestamp():
    client = bigquery.Client()
    query = """
    SELECT MAX(last_timestamp) as last_timestamp
    FROM `hot-or-not-feed-intelligence.analytics_views.userVideoRelation`
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row['last_timestamp']

def create_initial_query():
    return """
    CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.analytics_views.userVideoRelation` AS
    SELECT 
      JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
      JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
      AVG(CAST(JSON_EXTRACT_SCALAR(params, '$.percentage_watched') AS FLOAT64)) AS mean_percentage_watched,
      COUNT(*) AS total_count,
      MAX(timestamp) AS last_timestamp
    FROM 
      analytics_335143420.test_events_analytics
    WHERE 
      event = 'video_duration_watched'
    GROUP BY 
      user_id, video_id
    HAVING 
     user_id IS NOT NULL AND video_id IS NOT NULL AND mean_percentage_watched IS NOT NULL
    """
def create_incremental_query(last_timestamp):
    return f"""
    MERGE `hot-or-not-feed-intelligence.analytics_views.userVideoRelation` T
    USING (
      SELECT 
        JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
        AVG(CAST(JSON_EXTRACT_SCALAR(params, '$.percentage_watched') AS FLOAT64)) AS mean_percentage_watched,
        COUNT(*) AS total_count,
        MAX(timestamp) AS last_timestamp
      FROM 
        analytics_335143420.test_events_analytics
      WHERE 
        event = 'video_duration_watched' AND timestamp > '{last_timestamp}'
      GROUP BY 
        user_id, video_id
      HAVING 
       user_id IS NOT NULL AND video_id IS NOT NULL AND mean_percentage_watched IS NOT NULL
    ) S
    ON T.user_id = S.user_id AND T.video_id = S.video_id
    WHEN MATCHED THEN
      UPDATE SET 
        T.mean_percentage_watched = (T.mean_percentage_watched * T.total_count + S.mean_percentage_watched * S.total_count) / (T.total_count + S.total_count),
        T.total_count = T.total_count + S.total_count,
        T.last_timestamp = S.last_timestamp
    WHEN NOT MATCHED THEN
      INSERT (user_id, video_id, mean_percentage_watched, total_count, last_timestamp)
      VALUES (S.user_id, S.video_id, S.mean_percentage_watched, S.total_count, S.last_timestamp)
    """

def run_query():
    if check_table_exists():
        last_timestamp = get_last_timestamp()
        query = create_incremental_query(last_timestamp)
    else:
        query = create_initial_query()
    
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG('user_video_interaction_dag', default_args=default_args, schedule_interval='*/15 * * * *') as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query
    )