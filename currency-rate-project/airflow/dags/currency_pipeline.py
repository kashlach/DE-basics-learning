import sys
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.append('/opt/airflow/src')

from elt.loader import count_records, ensure_tables, get_engine, load_date


def ensure_raw_schema():
    get_engine()
    ensure_tables()

def extract_and_load(**context):
    target_date = datetime.fromisoformat(context['ds'])
    # context['ds'] - логич. дата, не дата факт. выпол-я, т.е. вчера
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
    description='Ежедневная загрузка курсов валют ЦБ РФ',
    schedule='0 10 * * *', # 10:00 UTC, т.е. 13 по МСК
    catchup=False,
    max_active_runs=1,
    tags=['raw', 'currency']
) as dag:

    # загрузка данных в БД raw слой
    # extract
    prepare = PythonOperator(task_id='prepare_db', python_callable=ensure_raw_schema)
    load = PythonOperator(task_id='load_rates', python_callable=extract_and_load)
    check = PythonOperator(task_id='check_results', python_callable=check_data_loaded)

    # dbt обработка ответа
    # transform и load
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/currency_dbt && dbt run --target prod'
    )

    # тестируем загруженные данные
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/currency_dbt && dbt test --target prod'
    )

prepare >> load >> check >> dbt_run >> dbt_test
