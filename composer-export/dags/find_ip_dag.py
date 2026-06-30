from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib.request

def get_outbound_ip():
    ip = urllib.request.urlopen('https://api.ipify.org').read().decode('utf-8')
    print(f"COMPOSER_OUTBOUND_IP: {ip}")
    return ip

with DAG('find_composer_ip', start_date=datetime(2024,1,1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id='get_ip', python_callable=get_outbound_ip)
