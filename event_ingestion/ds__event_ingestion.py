from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from google.cloud import storage
import os

# -----------------------------------------------------------------------------
# DAG & GCP configuration
# -----------------------------------------------------------------------------
DAG_ID = "event_ingestion_daily"
PROJECT_ID = "hot-or-not-feed-intelligence"
REGION = "us-central1"
CLUSTER_NAME = "event-ingestion-cluster"  # Static name; auto-deleted after 1 h
GCS_BUCKET = "yral-ds-dataproc-ce-staging"  # Update if needed

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def send_alert_to_google_chat(context):
    """Send a simple failure alert to Google Chat via webhook."""
    # Webhook URL removed for security
    print(f"DAG {DAG_ID} failed. Task: {context.get('task_instance').task_id}")


def upload_pyspark_script(**kwargs):
    """Upload the local PySpark script to the staging GCS bucket and return its URI."""
    local_path = os.path.join(os.path.dirname(__file__), "event_ingestion_script.py")
    with open(local_path, "r", encoding="utf-8") as file:
        script_body = file.read()

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob("pyspark_scripts/event_ingestion_script.py")
    blob.upload_from_string(script_body)

    gcs_uri = f"gs://{GCS_BUCKET}/pyspark_scripts/event_ingestion_script.py"
    print(f"Uploaded PySpark script to {gcs_uri}")
    return gcs_uri


# -----------------------------------------------------------------------------
# Dataproc cluster configuration (auto-delete after 1 hour)
# -----------------------------------------------------------------------------
CLUSTER_CONFIG = {
    "project_id": PROJECT_ID,
    "cluster_name": CLUSTER_NAME,
    "config": {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-4",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 100,
            },
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-4",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 100,
            },
        },
        "software_config": {
            "image_version": "2.0-debian10",
            "properties": {
                "spark:spark.jars.packages": (
                    "com.google.cloud.spark:" "spark-bigquery-with-dependencies_2.12:0.28.0"
                )
            },
        },
        # Auto-delete the cluster 1 hour (3600 s) after creation regardless of state
        "lifecycle_config": {
            "auto_delete_ttl": {"seconds": 2*3600},
        },
    },
    "region": REGION,
}

# -----------------------------------------------------------------------------
# PySpark job configuration – main file URI injected via XCom
# -----------------------------------------------------------------------------
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "{{ task_instance.xcom_pull(task_ids='upload_pyspark_script') }}",
        "properties": {"spark.submit.deployMode": "cluster"},
    },
}

# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id=DAG_ID,
    description="Create a temporary Dataproc cluster, process latest 100 analytics events, and auto-delete after 1 hour.",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["event_ingestion", "dataproc", "pyspark"],
) as dag:

    # 1. Upload PySpark script to GCS
    upload_pyspark_script_task = PythonOperator(
        task_id="upload_pyspark_script",
        python_callable=upload_pyspark_script,
        on_failure_callback=send_alert_to_google_chat,
    )

    # 2. Create Dataproc cluster (auto-deletes after 1 h via lifecycle_config)
    create_cluster_task = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        on_failure_callback=send_alert_to_google_chat,
    )

    # 3. Submit PySpark job
    submit_pyspark_job_task = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=send_alert_to_google_chat,
    )

    # Task dependencies
    upload_pyspark_script_task >> create_cluster_task >> submit_pyspark_job_task 