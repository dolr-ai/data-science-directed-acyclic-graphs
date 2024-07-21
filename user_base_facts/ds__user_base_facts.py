from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from google.cloud import bigquery

init_ubf_query = """
CREATE OR REPLACE TABLE `yral_ds.user_base_facts`
CLUSTER BY user_id AS                             
SELECT
    user_id,
    user_info,
    device,
    geo,
    audiences,
    user_properties,
    user_ltv,
    predictions,
    privacy_info,
    occurrence_date,
    PARSE_DATE('%Y%m%d', CAST(last_updated_date AS STRING)) AS last_updated_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_updated_date DESC) AS row_num
    FROM
        `analytics_434929785.users_*`
)
WHERE row_num = 1
"""



update_ubf_query = """
MERGE `yral_ds.user_base_facts` T
USING (
    SELECT
        user_id,
        user_info,
        device,
        geo,
        audiences,
        user_properties,
        user_ltv,
        predictions,
        privacy_info,
        occurrence_date,
        PARSE_DATE('%Y%m%d', CAST(last_updated_date AS STRING)) AS last_updated_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY last_updated_date DESC) AS row_num
        FROM
            `analytics_434929785.users_*`
        WHERE
            PARSE_DATE('%Y%m%d', CAST(last_updated_date AS STRING)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    )
    WHERE row_num = 1
) S
ON T.user_id = S.user_id
WHEN MATCHED THEN
    UPDATE SET
        T.user_info = S.user_info,
        T.device = S.device,
        T.geo = S.geo,
        T.audiences = S.audiences,
        T.user_properties = S.user_properties,
        T.user_ltv = S.user_ltv,
        T.predictions = S.predictions,
        T.privacy_info = S.privacy_info,
        T.occurrence_date = S.occurrence_date,
        T.last_updated_date = S.last_updated_date
WHEN NOT MATCHED THEN
    INSERT (
        user_id,
        user_info,
        device,
        geo,
        audiences,
        user_properties,
        user_ltv,
        predictions,
        privacy_info,
        occurrence_date,
        last_updated_date
    )
    VALUES (
        S.user_id,
        S.user_info,
        S.device,
        S.geo,
        S.audiences,
        S.user_properties,
        S.user_ltv,
        S.predictions,
        S.privacy_info,
        S.occurrence_date,
        S.last_updated_date
    );
"""

def check_table_exists():
    client = bigquery.Client()
    query = """
    SELECT COUNT(*)
    FROM `yral_ds.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'user_base_facts'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row[0] > 0
    
def updaet_or_init_ubf_table():
    if check_table_exists():
        query = update_ubf_query
    else:
        query = init_ubf_query
    
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('user_base_facts', default_args=default_args, schedule_interval='0 0 * * *', catchup=False) as dag:
    run_query_task = PythonOperator(
        task_id='run_query_task',
        python_callable=updaet_or_init_ubf_table
    )