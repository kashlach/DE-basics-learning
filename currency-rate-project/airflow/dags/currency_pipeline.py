import sys
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.append('/opt/airflow/src')

from elt.config import DATABASE_URL
from elt.loader import count_records, ensure_tables, get_engine, load_date


def ensure_raw_schema():
    get_engine(DATABASE_URL)
    ensure_tables()

def extract_and_load(**context):
    target_date = datetime.fromisoformat(context['ds'])
    load_date(target_date)

def check_data_loaded():
    total = count_records()
    if total == 0:
        raise Exception('В БД нет записей после загрузки!')

def_args = {
    'owner': 'pypipy',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'currency_pipeline',
    default_args=def_args,
    description='Инкрементальная загрузка staging для customers',
    schedule='0 10 * * *', # в 10 по МСК
    catchup=False,
    max_active_runs=1,
    tags=['raw', 'currency']
) as dag:

    prepare = PythonOperator(task_id='prepare_db', python_callable=ensure_raw_schema)
    load = PythonOperator(task_id='load_rates', python_callable=extract_and_load)
    check = PythonOperator(task_id='check_results', python_callable=check_data_loaded)

prepare >> load >> check
