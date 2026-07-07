import logging
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

def form_weekly_rep(**context):
    '''
    Эксель отчет с двумя листами: детальная информация и сводный.
    Данные по курсам определенных валют за три месяца.
    Отчет по пн, данные "обновляются" прошедшей неделей.
    '''

    #logical_date = datetime.fromisoformat(context['ds'])
    logical_date = context['ds'] # str YYYY-MM-DD

    if datetime.fromisoformat(logical_date).isoweekday() != 1:
        logging.info('Не Пн, отчет не формируем')
        return

    logging.info('Формируем еженедельный отчет')
    import pandas as pd
    from sqlalchemy import text

    engine = get_engine()
    df = pd.read_sql(text('''
        SELECT *, EXTRACT(WEEK FROM requested_date) AS week_num
        FROM finance_marts.dm_weekly_report
        WHERE requested_date >= CAST(:dt AS DATE) - INTERVAL '3' MONTH
          AND requested_date <= CAST(:dt AS DATE)
        ORDER BY requested_date DESC, char_code ASC
    '''),
    engine,
    params={'dt': logical_date})

    df_pivot = pd.pivot_table(
        df,
        values=['rate'],
        index=['char_code'],
        columns=['week_num'],
        aggfunc='mean'
    )

    # - ./airflow/reports:/opt/airflow/reports/
    write_path = f'/opt/airflow/reports/weekly_report_{logical_date}.xlsx'
    with pd.ExcelWriter(write_path, engine='openpyxl') as writer:
        df_pivot.to_excel(writer, sheet_name='svod')
        df.to_excel(writer, sheet_name='detailed')

    # TODO: отправка на почту

def run_usd_alert(**context):
    from sqlalchemy import text

    check_date = context['ds']

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text('''
            SELECT current_rate, prev_date_rate, daily_change_pct
            FROM finance_marts.dm_change_alerts
            WHERE requested_date = CAST(:dt AS DATE)
              AND alert_triggered = TRUE
        '''), {'dt': check_date}).fetchone()

        if not (row and row[0]):
            logging.info('С курсом USD все нормально')
            return

        logging.info('Скачок курса USD!')
        logging.info(f'Был {row[0]}, стал {row[1]} -> изменение на {row[2]} %')
    # TODO: отправка на почту


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

    # excel отчеты
    weekly_rep = PythonOperator(task_id='form_rep', python_callable=form_weekly_rep)
    alert = PythonOperator(task_id='usd_alert', python_callable=run_usd_alert)


prepare >> load >> check >> dbt_run >> dbt_test >> [weekly_rep, alert]
