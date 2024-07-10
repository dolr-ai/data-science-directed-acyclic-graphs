from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from google.cloud import dataproc_v1

DAG_ID = 'dataproc_pyspark_job_demo'
CLUSTER_NAME = 'yral-ds-dataproc-ce'
PROJECT_ID = 'hot-or-not-feed-intelligence'  
REGION = 'us-central1'

def get_cluster_config(**kwargs):
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    cluster = client.get_cluster(
        request={
            "project_id": PROJECT_ID,
            "region": REGION,
            "cluster_name": CLUSTER_NAME,
        }
    )
    return cluster.config.config_bucket

def upload_pyspark_file(**kwargs):
    from google.cloud import storage
    
    staging_bucket = kwargs['ti'].xcom_pull(task_ids='get_cluster_config')
    
    with open(os.path.join(os.path.dirname(__file__), 'demo_pyspark_script.py'), 'r') as file:
        pyspark_code = file.read()
    # Upload the script to the staging bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(staging_bucket)
    blob = bucket.blob('pyspark_scripts/hello_world.py')
    blob.upload_from_string(pyspark_code)
    
    return f'gs://{staging_bucket}/pyspark_scripts/hello_world.py'

with DAG(
    DAG_ID,
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    get_config = PythonOperator(
        task_id='get_cluster_config',
        python_callable=get_cluster_config,
    )

    upload_file = PythonOperator(
        task_id='upload_pyspark_file',
        python_callable=upload_pyspark_file,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": "{{ task_instance.xcom_pull(task_ids='upload_pyspark_file') }}"},
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    get_config >> upload_file >> submit_job