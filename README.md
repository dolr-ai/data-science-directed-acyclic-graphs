# Data Processing Pipeline Starter Guide

This repository contains various data processing pipelines built with Apache Airflow. This guide will help you get started with creating your own data pipelines using different technologies.

## Prerequisites

- Python 3.8+
- Apache Airflow 2.0+
- Apache Spark (for Spark pipelines)
- Google Cloud Platform account (for BigQuery operations)
- Docker (recommended for local development)

## Setup Instructions

1. Clone this repository
2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up Airflow:
```bash
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

## Quick Start Guides

### 1. Simple Python DAG (Daily Active Users Example)

Create a new file `dags/daily_active_users.py`:

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

def calculate_dau():
    # Sample code - replace with your actual data source
    df = pd.read_csv('user_logs.csv')
    dau = df['user_id'].nunique()
    print(f"Daily Active Users: {dau}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'daily_active_users',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False
) as dag:

    calculate_dau_task = PythonOperator(
        task_id='calculate_dau',
        python_callable=calculate_dau
    )
```

### 2. Spark Pipeline Example

Create a new file `dags/spark_example.py`:

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    'spark_processing_example',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:

    spark_job = SparkSubmitOperator(
        task_id='spark_job',
        application='spark_jobs/process_data.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g'
        }
    )
```

### 3. ML Model Pipeline Example

Create a new file `dags/ml_training_pipeline.py`:

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import pandas as pd

def train_model():
    # Sample ML training code
    data = pd.read_csv('training_data.csv')
    X = data.drop('target', axis=1)
    y = data['target']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y)
    model = RandomForestClassifier()
    model.fit(X_train, y_train)
    score = model.score(X_test, y_test)
    print(f"Model accuracy: {score}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    'ml_training_pipeline',
    default_args=default_args,
    schedule_interval='@weekly'
) as dag:

    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )
```

### 4. SQL Query Pipeline Example

Create a new file `dags/sql_pipeline.py`:

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    'sql_pipeline_example',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:

    run_query = BigQueryExecuteQueryOperator(
        task_id='run_daily_analysis',
        sql="""
            SELECT 
                DATE(timestamp) as date,
                COUNT(DISTINCT user_id) as daily_users
            FROM `your_project.your_dataset.events`
            WHERE DATE(timestamp) = CURRENT_DATE()
            GROUP BY DATE(timestamp)
        """,
        use_legacy_sql=False,
        destination_dataset_table='your_project.your_dataset.daily_metrics'
    )
```
## CICD
Refer `.github/ci.yaml` to import your newly created dag.
This has to be edited if the dag is created for the first time

## Running Your First Pipeline

1. Start the Airflow webserver:
```bash
airflow webserver --port 8080
```

2. In a new terminal, start the Airflow scheduler:
```bash
airflow scheduler
```

3. Visit `http://localhost:8080` in your browser
4. Login with the credentials you created
5. Enable your DAG in the Airflow UI
6. Monitor your pipeline's execution

## Best Practices

1. Always use virtual environments
2. Keep your DAGs simple and modular
3. Use appropriate retry mechanisms
4. Implement proper error handling and alerting
5. Follow the Airflow best practices for task dependencies
6. Use variables and connections in Airflow for sensitive data

## Troubleshooting

Common issues and solutions:
- If Airflow webserver doesn't start, check if port 8080 is available
- Ensure all required dependencies are installed
- Check Airflow logs in `$AIRFLOW_HOME/logs/`
- Verify database connections and credentials

## Support

For questions and support, please open an issue in the repository.
