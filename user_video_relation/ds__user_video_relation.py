from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from google.cloud import bigquery

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `hot-or-not-feed-intelligence.yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'userVideoRelation'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0

def get_last_timestamp():
    client = bigquery.Client()
    query = """
    SELECT MAX(last_watched_timestamp) as last_watched_timestamp
    FROM `hot-or-not-feed-intelligence.yral_ds.userVideoRelation`
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row['last_watched_timestamp']

def create_initial_query():
    return """
    CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.userVideoRelation` AS
    WITH video_watched AS (
      SELECT 
        JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
        max(timestamp) as last_watched_timestamp,
        AVG(CAST(JSON_EXTRACT_SCALAR(params, '$.percentage_watched') AS FLOAT64)) AS mean_percentage_watched
      FROM 
        analytics_335143420.test_events_analytics -- base analytics table -- change this if the table name changes
      WHERE 
        event = 'video_duration_watched'
      GROUP BY 
        user_id, video_id
    ),
    video_liked AS (
      SELECT 
        JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
        max(timestamp) as last_liked_timestamp,
        TRUE AS liked
      FROM 
        analytics_335143420.test_events_analytics
      WHERE 
        event = 'like_video'
      GROUP BY 
        user_id, video_id
    )
    SELECT 
      vw.user_id,
      vw.video_id,
      vw.last_watched_timestamp,
      vw.mean_percentage_watched,
      vl.last_liked_timestamp,
      COALESCE(vl.liked, FALSE) AS liked
    FROM 
      video_watched vw
    LEFT JOIN 
      video_liked vl
    ON 
      vw.user_id = vl.user_id AND vw.video_id = vl.video_id
    order by last_watched_timestamp desc;
    """

def create_incremental_query(last_timestamp):
    return f"""
    MERGE `hot-or-not-feed-intelligence.yral_ds.userVideoRelation` T
    USING (
      WITH video_watched AS (
        SELECT 
          JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
          JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
          max(timestamp) as last_watched_timestamp,
          AVG(CAST(JSON_EXTRACT_SCALAR(params, '$.percentage_watched') AS FLOAT64)) AS mean_percentage_watched
        FROM 
          analytics_335143420.test_events_analytics
        WHERE 
          event = 'video_duration_watched'
          and timestamp > '{last_timestamp}'
        GROUP BY
          user_id, video_id
      ),
      video_liked AS (
        SELECT 
          JSON_EXTRACT_SCALAR(params, '$.user_id') AS user_id,
          JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
          max(timestamp) as last_liked_timestamp,
          TRUE AS liked
        FROM 
          analytics_335143420.test_events_analytics
        WHERE 
          event = 'like_video'
          and timestamp > '{last_timestamp}'
        GROUP BY 
          user_id, video_id
      )
      SELECT 
        vw.user_id,
        vw.video_id,
        vw.last_watched_timestamp,
        vw.mean_percentage_watched,
        vl.last_liked_timestamp,
        COALESCE(vl.liked, FALSE) AS liked
      FROM 
        video_watched vw
      LEFT JOIN 
        video_liked vl
      ON 
        vw.user_id = vl.user_id AND vw.video_id = vl.video_id
      ORDER BY 
        vw.last_watched_timestamp DESC
    ) S
    ON T.user_id = S.user_id AND T.video_id = S.video_id
    WHEN MATCHED THEN
      UPDATE SET 
        T.mean_percentage_watched = S.mean_percentage_watched,
        T.last_watched_timestamp = S.last_watched_timestamp,
        T.last_liked_timestamp = S.last_liked_timestamp,
        T.liked = T.liked OR S.liked
    WHEN NOT MATCHED THEN
      INSERT (user_id, video_id, last_watched_timestamp, mean_percentage_watched, last_liked_timestamp, liked)
      VALUES (S.user_id, S.video_id, S.last_watched_timestamp, S.mean_percentage_watched, S.last_liked_timestamp, S.liked)
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
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('user_video_interaction_dag', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=run_query
    )