from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def raise_test_failure() -> None:
    raise RuntimeError("Intentional Airflow Sentry verification failure")


with DAG(
    dag_id="ds__sentry_verification",
    description="Manual-only DAG to verify Airflow Sentry integration",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "platform", "retries": 0},
    tags=["verification", "sentry"],
) as dag:
    PythonOperator(
        task_id="raise_test_failure",
        python_callable=raise_test_failure,
    )
