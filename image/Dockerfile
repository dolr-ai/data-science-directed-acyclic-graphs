FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

COPY image/requirements-airflow-2.10.5.txt /requirements.txt

RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" -r /requirements.txt

COPY composer-export/dags/ /opt/airflow/dags/
COPY composer-export/plugins/ /opt/airflow/plugins/

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins
ENV PYTHONPATH=/opt/airflow/dags:/opt/airflow/plugins

WORKDIR /opt/airflow
