from airflow import DAG
from airflow.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'global_popular_videos_l7d',
    default_args=default_args,
    description='A DAG to calculate global popular videos for the last 7 days',
    schedule_interval='10 0 * * *',  # Runs every day at 00:00:10
    catchup=False,
)

# Define the BigQuery SQL query
sql_query = """
CREATE OR REPLACE TABLE `hot-or-not-feed-intelligence.yral_ds.global_popular_videos_l7d` AS
WITH stats AS (
    SELECT 
        video_id,
        AVG(CAST(liked AS INT64)) * 100 AS like_perc,
        AVG(mean_percentage_watched) AS watch_perc
    FROM 
        `hot-or-not-feed-intelligence.yral_ds.userVideoRelation`
    WHERE 
        last_watched_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY 
        video_id
),
stats_with_mean_std AS (
    SELECT
        video_id,
        like_perc,
        watch_perc,
        AVG(like_perc) OVER() AS mean_like_perc,
        STDDEV(like_perc) OVER() AS stddev_like_perc,
        AVG(watch_perc) OVER() AS mean_watch_perc,
        STDDEV(watch_perc) OVER() AS stddev_watch_perc
    FROM
        stats
)
SELECT
    video_id,
    (like_perc - mean_like_perc) / stddev_like_perc AS normalized_like_perc,
    (watch_perc - mean_watch_perc) / stddev_watch_perc AS normalized_watch_perc,
    ((like_perc - mean_like_perc) / stddev_like_perc + (watch_perc - mean_watch_perc) / stddev_watch_perc) / 2 AS global_popularity_score
FROM
    stats_with_mean_std
ORDER BY
    global_popularity_score DESC
"""

# Define the BigQuery task
bq_task = BigQueryOperator(
    task_id='run_global_popular_videos_query',
    sql=sql_query,
    use_legacy_sql=False,
    dag=dag,
)

# Set the task dependencies
bq_task